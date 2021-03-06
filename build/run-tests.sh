#!/bin/bash
# Copyright (C) The Arvados Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

. `dirname "$(readlink -f "$0")"`/libcloud-pin.sh

COLUMNS=80
. `dirname "$(readlink -f "$0")"`/run-library.sh

read -rd "\000" helpmessage <<EOF
$(basename $0): Install and test Arvados components.

Exit non-zero if any tests fail.

Syntax:
        $(basename $0) WORKSPACE=/path/to/arvados [options]

Options:

--skip FOO     Do not test the FOO component.
--only FOO     Do not test anything except the FOO component.
--temp DIR     Install components and dependencies under DIR instead of
               making a new temporary directory. Implies --leave-temp.
--leave-temp   Do not remove GOPATH, virtualenv, and other temp dirs at exit.
               Instead, show the path to give as --temp to reuse them in
               subsequent invocations.
--repeat N     Repeat each install/test step until it succeeds N times.
--retry        Prompt to retry if an install or test suite fails.
--skip-install Do not run any install steps. Just run tests.
               You should provide GOPATH, GEMHOME, and VENVDIR options
               from a previous invocation if you use this option.
--only-install Run specific install step
--short        Skip (or scale down) some slow tests.
WORKSPACE=path Arvados source tree to test.
CONFIGSRC=path Dir with api server config files to copy into source tree.
               (If none given, leave config files alone in source tree.)
services/api_test="TEST=test/functional/arvados/v1/collections_controller_test.rb"
               Restrict apiserver tests to the given file
sdk/python_test="--test-suite test.test_keep_locator"
               Restrict Python SDK tests to the given class
apps/workbench_test="TEST=test/integration/pipeline_instances_test.rb"
               Restrict Workbench tests to the given file
services/arv-git-httpd_test="-check.vv"
               Show all log messages, even when tests pass (also works
               with services/keepstore_test etc.)
ARVADOS_DEBUG=1
               Print more debug messages
envvar=value   Set \$envvar to value. Primarily useful for WORKSPACE,
               *_test, and other examples shown above.

Assuming --skip-install is not given, all components are installed
into \$GOPATH, \$VENDIR, and \$GEMHOME before running any tests. Many
test suites depend on other components being installed, and installing
everything tends to be quicker than debugging dependencies.

As a special concession to the current CI server config, CONFIGSRC
defaults to $HOME/arvados-api-server if that directory exists.

More information and background:

https://arvados.org/projects/arvados/wiki/Running_tests

Available tests:

apps/workbench (*)
apps/workbench_units (*)
apps/workbench_functionals (*)
apps/workbench_integration (*)
apps/workbench_benchmark
apps/workbench_profile
cmd/arvados-client
cmd/arvados-server
doc
lib/cli
lib/cmd
lib/controller
lib/crunchstat
lib/dispatchcloud
services/api
services/arv-git-httpd
services/crunchstat
services/dockercleaner
services/fuse
services/health
services/keep-web
services/keepproxy
services/keepstore
services/keep-balance
services/login-sync
services/nodemanager
services/nodemanager_integration
services/crunch-run
services/crunch-dispatch-local
services/crunch-dispatch-slurm
services/ws
sdk/cli
sdk/pam
sdk/python
sdk/python:py3
sdk/ruby
sdk/go/arvados
sdk/go/arvadosclient
sdk/go/dispatch
sdk/go/keepclient
sdk/go/health
sdk/go/httpserver
sdk/go/manifest
sdk/go/blockdigest
sdk/go/asyncbuf
sdk/go/stats
sdk/go/crunchrunner
sdk/cwl
sdk/R
tools/sync-groups
tools/crunchstat-summary
tools/keep-exercise
tools/keep-rsync
tools/keep-block-check

(*) apps/workbench is shorthand for apps/workbench_units +
    apps/workbench_functionals + apps/workbench_integration

EOF

# First make sure to remove any ARVADOS_ variables from the calling
# environment that could interfere with the tests.
unset $(env | cut -d= -f1 | grep \^ARVADOS_)

# Reset other variables that could affect our [tests'] behavior by
# accident.
GITDIR=
GOPATH=
VENVDIR=
VENV3DIR=
PYTHONPATH=
GEMHOME=
PERLINSTALLBASE=
R_LIBS=

short=
only_install=
temp=
temp_preserve=

clear_temp() {
    if [[ -z "$temp" ]]; then
        # we didn't even get as far as making a temp dir
        :
    elif [[ -z "$temp_preserve" ]]; then
        rm -rf "$temp"
    else
        echo "Leaving behind temp dirs in $temp"
    fi
}

fatal() {
    clear_temp
    echo >&2 "Fatal: $* (encountered in ${FUNCNAME[1]} at ${BASH_SOURCE[1]} line ${BASH_LINENO[0]})"
    exit 1
}

exit_cleanly() {
    trap - INT
    create-plot-data-from-log.sh $BUILD_NUMBER "$WORKSPACE/apps/workbench/log/test.log" "$WORKSPACE/apps/workbench/log/"
    rotate_logfile "$WORKSPACE/apps/workbench/log/" "test.log"
    stop_services
    rotate_logfile "$WORKSPACE/services/api/log/" "test.log"
    report_outcomes
    clear_temp
    exit ${#failures}
}

sanity_checks() {
    ( [[ -n "$WORKSPACE" ]] && [[ -d "$WORKSPACE/services" ]] ) \
        || fatal "WORKSPACE environment variable not set to a source directory (see: $0 --help)"
    echo Checking dependencies:
    echo -n 'virtualenv: '
    virtualenv --version \
        || fatal "No virtualenv. Try: apt-get install virtualenv (on ubuntu: python-virtualenv)"
    echo -n 'ruby: '
    ruby -v \
        || fatal "No ruby. Install >=2.1.9 (using rbenv, rvm, or source)"
    echo -n 'go: '
    go version \
        || fatal "No go binary. See http://golang.org/doc/install"
    [[ $(go version) =~ go1.([0-9]+) ]] && [[ ${BASH_REMATCH[1]} -ge 10 ]] \
        || fatal "Go >= 1.10 required. See http://golang.org/doc/install"
    echo -n 'gcc: '
    gcc --version | egrep ^gcc \
        || fatal "No gcc. Try: apt-get install build-essential"
    echo -n 'fuse.h: '
    find /usr/include -path '*fuse/fuse.h' | egrep --max-count=1 . \
        || fatal "No fuse/fuse.h. Try: apt-get install libfuse-dev"
    echo -n 'gnutls.h: '
    find /usr/include -path '*gnutls/gnutls.h' | egrep --max-count=1 . \
        || fatal "No gnutls/gnutls.h. Try: apt-get install libgnutls28-dev"
    echo -n 'Python2 pyconfig.h: '
    find /usr/include -path '*/python2*/pyconfig.h' | egrep --max-count=1 . \
        || fatal "No Python2 pyconfig.h. Try: apt-get install python2.7-dev"
    echo -n 'Python3 pyconfig.h: '
    find /usr/include -path '*/python3*/pyconfig.h' | egrep --max-count=1 . \
        || fatal "No Python3 pyconfig.h. Try: apt-get install python3-dev"
    echo -n 'nginx: '
    PATH="$PATH:/sbin:/usr/sbin:/usr/local/sbin" nginx -v \
        || fatal "No nginx. Try: apt-get install nginx"
    echo -n 'perl: '
    perl -v | grep version \
        || fatal "No perl. Try: apt-get install perl"
    for mod in ExtUtils::MakeMaker JSON LWP Net::SSL; do
        echo -n "perl $mod: "
        perl -e "use $mod; print \"\$$mod::VERSION\\n\"" \
            || fatal "No $mod. Try: apt-get install perl-modules libcrypt-ssleay-perl libjson-perl libwww-perl"
    done
    echo -n 'gitolite: '
    which gitolite \
        || fatal "No gitolite. Try: apt-get install gitolite3"
    echo -n 'npm: '
    npm --version \
        || fatal "No npm. Try: wget -O- https://nodejs.org/dist/v6.11.2/node-v6.11.2-linux-x64.tar.xz | sudo tar -C /usr/local -xJf - && sudo ln -s ../node-v6.11.2-linux-x64/bin/{node,npm} /usr/local/bin/"
    echo -n 'cadaver: '
    cadaver --version | grep -w cadaver \
          || fatal "No cadaver. Try: apt-get install cadaver"
    echo -n 'libattr1 xattr.h: '
    find /usr/include -path '*/attr/xattr.h' | egrep --max-count=1 . \
        || fatal "No libattr1 xattr.h. Try: apt-get install libattr1-dev"
    echo -n 'libcurl curl.h: '
    find /usr/include -path '*/curl/curl.h' | egrep --max-count=1 . \
        || fatal "No libcurl curl.h. Try: apt-get install libcurl4-gnutls-dev"
    echo -n 'libpq libpq-fe.h: '
    find /usr/include -path '*/postgresql/libpq-fe.h' | egrep --max-count=1 . \
        || fatal "No libpq libpq-fe.h. Try: apt-get install libpq-dev"
    echo -n 'services/api/config/database.yml: '
    if [[ ! -f "$WORKSPACE/services/api/config/database.yml" ]]; then
	    fatal "Please provide a database.yml file for the test suite"
    else
	    echo "OK"
    fi
    echo -n 'postgresql: '
    psql --version || fatal "No postgresql. Try: apt-get install postgresql postgresql-client-common"
    echo -n 'phantomjs: '
    phantomjs --version || fatal "No phantomjs. Try: apt-get install phantomjs"
    echo -n 'xvfb: '
    which Xvfb || fatal "No xvfb. Try: apt-get install xvfb"
    echo -n 'graphviz: '
    dot -V || fatal "No graphviz. Try: apt-get install graphviz"

    if [[ "$NEED_SDK_R" = true ]]; then
      # R SDK stuff
      echo -n 'R: '
      which Rscript || fatal "No Rscript. Try: apt-get install r-base"
      echo -n 'testthat: '
      Rscript -e "library('testthat')" || fatal "No testthat. Try: apt-get install r-cran-testthat"
      # needed for roxygen2, needed for devtools, needed for R sdk
      pkg-config --exists libxml-2.0 || fatal "No libxml2. Try: apt-get install libxml2-dev"
      # needed for pkgdown, builds R SDK doc pages
      which pandoc || fatal "No pandoc. Try: apt-get install pandoc"
    fi
}

rotate_logfile() {
  # i.e.  rotate_logfile "$WORKSPACE/apps/workbench/log/" "test.log"
  # $BUILD_NUMBER is set by Jenkins if this script is being called as part of a Jenkins run
  if [[ -f "$1/$2" ]]; then
    THEDATE=`date +%Y%m%d%H%M%S`
    mv "$1/$2" "$1/$THEDATE-$BUILD_NUMBER-$2"
    gzip "$1/$THEDATE-$BUILD_NUMBER-$2"
  fi
}

declare -a failures
declare -A skip
declare -A testargs
skip[apps/workbench_profile]=1
# nodemanager_integration tests are not reliable, see #12061.
skip[services/nodemanager_integration]=1

while [[ -n "$1" ]]
do
    arg="$1"; shift
    case "$arg" in
        --help)
            echo >&2 "$helpmessage"
            echo >&2
            exit 1
            ;;
        --skip)
            skip[$1]=1; shift
            ;;
        --only)
            only="$1"; skip[$1]=""; shift
            ;;
        --short)
            short=1
            ;;
        --skip-install)
            only_install=nothing
            ;;
        --only-install)
            only_install="$1"; shift
            ;;
        --temp)
            temp="$1"; shift
            temp_preserve=1
            ;;
        --leave-temp)
            temp_preserve=1
            ;;
        --repeat)
            repeat=$((${1}+0)); shift
            ;;
        --retry)
            retry=1
            ;;
        *_test=*)
            suite="${arg%%_test=*}"
            args="${arg#*=}"
            testargs["$suite"]="$args"
            ;;
        *=*)
            eval export $(echo $arg | cut -d= -f1)=\"$(echo $arg | cut -d= -f2-)\"
            ;;
        *)
            echo >&2 "$0: Unrecognized option: '$arg'. Try: $0 --help"
            exit 1
            ;;
    esac
done

# R SDK installation is very slow (~360s in a clean environment) and only
# required when testing it. Skip that step if it is not needed.
NEED_SDK_R=true

if [[ ! -z "${only}" && "${only}" != "sdk/R" ]]; then
  NEED_SDK_R=false
fi

if [[ ! -z "${skip}" && "${skip}" == "sdk/R" ]]; then
  NEED_SDK_R=false
fi

start_services() {
    echo 'Starting API, keepproxy, keep-web, ws, arv-git-httpd, and nginx ssl proxy...'
    if [[ ! -d "$WORKSPACE/services/api/log" ]]; then
	mkdir -p "$WORKSPACE/services/api/log"
    fi
    # Remove empty api.pid file if it exists
    if [[ -f "$WORKSPACE/tmp/api.pid" && ! -s "$WORKSPACE/tmp/api.pid" ]]; then
	rm -f "$WORKSPACE/tmp/api.pid"
    fi
    cd "$WORKSPACE" \
        && eval $(python sdk/python/tests/run_test_server.py start --auth admin || echo fail=1) \
        && export ARVADOS_TEST_API_HOST="$ARVADOS_API_HOST" \
        && export ARVADOS_TEST_API_INSTALLED="$$" \
        && python sdk/python/tests/run_test_server.py start_controller \
        && python sdk/python/tests/run_test_server.py start_keep_proxy \
        && python sdk/python/tests/run_test_server.py start_keep-web \
        && python sdk/python/tests/run_test_server.py start_arv-git-httpd \
        && python sdk/python/tests/run_test_server.py start_ws \
        && eval $(python sdk/python/tests/run_test_server.py start_nginx || echo fail=1) \
        && (env | egrep ^ARVADOS)
    if [[ -n "$fail" ]]; then
       return 1
    fi
}

stop_services() {
    if [[ -z "$ARVADOS_TEST_API_HOST" ]]; then
        return
    fi
    unset ARVADOS_TEST_API_HOST
    cd "$WORKSPACE" \
        && python sdk/python/tests/run_test_server.py stop_nginx \
        && python sdk/python/tests/run_test_server.py stop_arv-git-httpd \
        && python sdk/python/tests/run_test_server.py stop_ws \
        && python sdk/python/tests/run_test_server.py stop_keep-web \
        && python sdk/python/tests/run_test_server.py stop_keep_proxy \
        && python sdk/python/tests/run_test_server.py stop_controller \
        && python sdk/python/tests/run_test_server.py stop
}

interrupt() {
    failures+=("($(basename $0) interrupted)")
    exit_cleanly
}
trap interrupt INT

sanity_checks

echo "WORKSPACE=$WORKSPACE"

if [[ -z "$CONFIGSRC" ]] && [[ -d "$HOME/arvados-api-server" ]]; then
    # Jenkins expects us to use this by default.
    CONFIGSRC="$HOME/arvados-api-server"
fi

# Clean up .pyc files that may exist in the workspace
cd "$WORKSPACE"
find -name '*.pyc' -delete

if [[ -z "$temp" ]]; then
    temp="$(mktemp -d)"
fi

# Set up temporary install dirs (unless existing dirs were supplied)
for tmpdir in VENVDIR VENV3DIR GOPATH GEMHOME PERLINSTALLBASE R_LIBS
do
    if [[ -z "${!tmpdir}" ]]; then
        eval "$tmpdir"="$temp/$tmpdir"
    fi
    if ! [[ -d "${!tmpdir}" ]]; then
        mkdir "${!tmpdir}" || fatal "can't create ${!tmpdir} (does $temp exist?)"
    fi
done

rm -vf "${WORKSPACE}/tmp/*.log"

setup_ruby_environment() {
    if [[ -s "$HOME/.rvm/scripts/rvm" ]] ; then
        source "$HOME/.rvm/scripts/rvm"
        using_rvm=true
    elif [[ -s "/usr/local/rvm/scripts/rvm" ]] ; then
        source "/usr/local/rvm/scripts/rvm"
        using_rvm=true
    else
        using_rvm=false
    fi

    if [[ "$using_rvm" == true ]]; then
        # If rvm is in use, we can't just put separate "dependencies"
        # and "gems-under-test" paths to GEM_PATH: passenger resets
        # the environment to the "current gemset", which would lose
        # our GEM_PATH and prevent our test suites from running ruby
        # programs (for example, the Workbench test suite could not
        # boot an API server or run arv). Instead, we have to make an
        # rvm gemset and use it for everything.

        [[ `type rvm | head -n1` == "rvm is a function" ]] \
            || fatal 'rvm check'

        # Put rvm's favorite path back in first place (overriding
        # virtualenv, which just put itself there). Ignore rvm's
        # complaint about not being in first place already.
        rvm use @default 2>/dev/null

        # Create (if needed) and switch to an @arvados-tests-* gemset,
        # salting the gemset name so it doesn't interfere with
        # concurrent builds in other workspaces. Leave the choice of
        # ruby to the caller.
        gemset="arvados-tests-$(echo -n "${WORKSPACE}" | md5sum | head -c16)"
        rvm use "@${gemset}" --create \
            || fatal 'rvm gemset setup'

        rvm env
    else
        # When our "bundle install"s need to install new gems to
        # satisfy dependencies, we want them to go where "gem install
        # --user-install" would put them. (However, if the caller has
        # already set GEM_HOME, we assume that's where dependencies
        # should be installed, and we should leave it alone.)

        if [ -z "$GEM_HOME" ]; then
            user_gempath="$(gem env gempath)"
            export GEM_HOME="${user_gempath%%:*}"
        fi
        PATH="$(gem env gemdir)/bin:$PATH"

        # When we build and install our own gems, we install them in our
        # $GEMHOME tmpdir, and we want them to be at the front of GEM_PATH and
        # PATH so integration tests prefer them over other versions that
        # happen to be installed in $user_gempath, system dirs, etc.

        tmpdir_gem_home="$(env - PATH="$PATH" HOME="$GEMHOME" gem env gempath | cut -f1 -d:)"
        PATH="$tmpdir_gem_home/bin:$PATH"
        export GEM_PATH="$tmpdir_gem_home"

        echo "Will install dependencies to $(gem env gemdir)"
        echo "Will install arvados gems to $tmpdir_gem_home"
        echo "Gem search path is GEM_PATH=$GEM_PATH"
    fi
    bundle config || gem install bundler \
        || fatal 'install bundler'
}

with_test_gemset() {
    if [[ "$using_rvm" == true ]]; then
        "$@"
    else
        GEM_HOME="$tmpdir_gem_home" GEM_PATH="$tmpdir_gem_home" "$@"
    fi
}

gem_uninstall_if_exists() {
    if gem list "$1\$" | egrep '^\w'; then
        gem uninstall --force --all --executables "$1"
    fi
}

setup_virtualenv() {
    local venvdest="$1"; shift
    if ! [[ -e "$venvdest/bin/activate" ]] || ! [[ -e "$venvdest/bin/pip" ]]; then
        virtualenv --setuptools "$@" "$venvdest" || fatal "virtualenv $venvdest failed"
    elif [[ -n "$short" ]]; then
        return
    fi
    if [[ $("$venvdest/bin/python" --version 2>&1) =~ \ 3\.[012]\. ]]; then
        # pip 8.0.0 dropped support for python 3.2, e.g., debian wheezy
        "$venvdest/bin/pip" install --no-cache-dir 'setuptools>=18.5' 'pip>=7,<8'
    else
        "$venvdest/bin/pip" install --no-cache-dir 'setuptools>=18.5' 'pip>=7'
    fi
    # ubuntu1404 can't seem to install mock via tests_require, but it can do this.
    "$venvdest/bin/pip" install --no-cache-dir 'mock>=1.0' 'pbr<1.7.0'
}

export PERLINSTALLBASE
export PERLLIB="$PERLINSTALLBASE/lib/perl5:${PERLLIB:+$PERLLIB}"

export R_LIBS

export GOPATH
(
    set -e
    mkdir -p "$GOPATH/src/git.curoverse.com"
    rmdir -v --parents --ignore-fail-on-non-empty "${temp}/GOPATH"
    if [[ ! -h "$GOPATH/src/git.curoverse.com/arvados.git" ]]; then
        for d in \
            "$GOPATH/src/git.curoverse.com/arvados.git/tmp/GOPATH" \
                "$GOPATH/src/git.curoverse.com/arvados.git/tmp" \
                "$GOPATH/src/git.curoverse.com/arvados.git"; do
            [[ -d "$d" ]] && rmdir "$d"
        done
    fi
    for d in \
        "$GOPATH/src/git.curoverse.com/arvados.git/arvados" \
        "$GOPATH/src/git.curoverse.com/arvados.git"; do
        [[ -h "$d" ]] && rm "$d"
    done
    ln -vsfT "$WORKSPACE" "$GOPATH/src/git.curoverse.com/arvados.git"
    go get -v github.com/kardianos/govendor
    cd "$GOPATH/src/git.curoverse.com/arvados.git"
    if [[ -n "$short" ]]; then
        go get -v -d ...
        "$GOPATH/bin/govendor" sync
    else
        # Remove cached source dirs in workdir. Otherwise, they will
        # not qualify as +missing or +external below, and we won't be
        # able to detect that they're missing from vendor/vendor.json.
        rm -rf vendor/*/
        go get -v -d ...
        "$GOPATH/bin/govendor" sync
        [[ -z $("$GOPATH/bin/govendor" list +unused +missing +external | tee /dev/stderr) ]] \
            || fatal "vendor/vendor.json has unused or missing dependencies -- try:

(export GOPATH=\"${GOPATH}\"; cd \$GOPATH/src/git.curoverse.com/arvados.git && \$GOPATH/bin/govendor add +missing +external && \$GOPATH/bin/govendor remove +unused)

";
    fi
) || fatal "Go setup failed"

setup_virtualenv "$VENVDIR" --python python2.7
. "$VENVDIR/bin/activate"

# Needed for run_test_server.py which is used by certain (non-Python) tests.
pip install --no-cache-dir PyYAML \
    || fatal "pip install PyYAML failed"

# Preinstall libcloud if using a fork; otherwise nodemanager "pip
# install" won't pick it up by default.
if [[ -n "$LIBCLOUD_PIN_SRC" ]]; then
    pip freeze 2>/dev/null | egrep ^apache-libcloud==$LIBCLOUD_PIN \
        || pip install --pre --ignore-installed --no-cache-dir "$LIBCLOUD_PIN_SRC" >/dev/null \
        || fatal "pip install apache-libcloud failed"
fi

# Deactivate Python 2 virtualenv
deactivate

declare -a pythonstuff
pythonstuff=(
    sdk/pam
    sdk/python
    sdk/python:py3
    sdk/cwl
    services/dockercleaner:py3
    services/fuse
    services/nodemanager
    tools/crunchstat-summary
    )

# If Python 3 is available, set up its virtualenv in $VENV3DIR.
# Otherwise, skip dependent tests.
PYTHON3=$(which python3)
if [[ ${?} = 0 ]]; then
    setup_virtualenv "$VENV3DIR" --python python3
else
    PYTHON3=
    cat >&2 <<EOF

Warning: python3 could not be found. Python 3 tests will be skipped.

EOF
fi

# Reactivate Python 2 virtualenv
. "$VENVDIR/bin/activate"

# Note: this must be the last time we change PATH, otherwise rvm will
# whine a lot.
setup_ruby_environment

echo "PATH is $PATH"

if ! which bundler >/dev/null
then
    gem install --user-install bundler || fatal 'Could not install bundler'
fi

# Jenkins config requires that glob tmp/*.log match something. Ensure
# that happens even if we don't end up running services that set up
# logging.
mkdir -p "${WORKSPACE}/tmp/" || fatal "could not mkdir ${WORKSPACE}/tmp"
touch "${WORKSPACE}/tmp/controller.log" || fatal "could not touch ${WORKSPACE}/tmp/controller.log"

retry() {
    remain="${repeat}"
    while :
    do
        if ${@}; then
            if [[ "$remain" -gt 1 ]]; then
                remain=$((${remain}-1))
                title "Repeating ${remain} more times"
            else
                break
            fi
        elif [[ "$retry" == 1 ]]; then
            read -p 'Try again? [Y/n] ' x
            if [[ "$x" != "y" ]] && [[ "$x" != "" ]]
            then
                break
            fi
        else
            break
        fi
    done
}

do_test() {
    case "${1}" in
        apps/workbench_units | apps/workbench_functionals | apps/workbench_integration)
            suite=apps/workbench
            ;;
        services/nodemanager | services/nodemanager_integration)
            suite=services/nodemanager_suite
            ;;
        *)
            suite="${1}"
            ;;
    esac
    if [[ -z "${skip[$suite]}" && -z "${skip[$1]}" && \
              (-z "${only}" || "${only}" == "${suite}" || \
                   "${only}" == "${1}") ||
                  "${only}" == "${2}" ]]; then
        retry do_test_once ${@}
    else
        title "Skipping ${1} tests"
    fi
}

do_test_once() {
    unset result

    title "Running $1 tests"
    timer_reset
    if [[ "$2" == "go" ]]
    then
        covername="coverage-$(echo "$1" | sed -e 's/\//_/g')"
        coverflags=("-covermode=count" "-coverprofile=$WORKSPACE/tmp/.$covername.tmp")
        # We do "go get -t" here to catch compilation errors
        # before trying "go test". Otherwise, coverage-reporting
        # mode makes Go show the wrong line numbers when reporting
        # compilation errors.
        go get -t "git.curoverse.com/arvados.git/$1" && \
            cd "$GOPATH/src/git.curoverse.com/arvados.git/$1" && \
            [[ -z "$(gofmt -e -d . | tee /dev/stderr)" ]] && \
            if [[ -n "${testargs[$1]}" ]]
        then
            # "go test -check.vv giturl" doesn't work, but this
            # does:
            go test ${short:+-short} ${testargs[$1]}
        else
            # The above form gets verbose even when testargs is
            # empty, so use this form in such cases:
            go test ${short:+-short} ${coverflags[@]} "git.curoverse.com/arvados.git/$1"
        fi
        result=${result:-$?}
        if [[ -f "$WORKSPACE/tmp/.$covername.tmp" ]]
        then
            go tool cover -html="$WORKSPACE/tmp/.$covername.tmp" -o "$WORKSPACE/tmp/$covername.html"
            rm "$WORKSPACE/tmp/.$covername.tmp"
        fi
    elif [[ "$2" == "pip" ]]
    then
        tries=0
        cd "$WORKSPACE/$1" && while :
        do
            tries=$((${tries}+1))
            # $3 can name a path directory for us to use, including trailing
            # slash; e.g., the bin/ subdirectory of a virtualenv.
            "${3}python" setup.py ${short:+--short-tests-only} test ${testargs[$1]}
            result=$?
            if [[ ${tries} < 3 && ${result} == 137 ]]
            then
                printf '\n*****\n%s tests killed -- retrying\n*****\n\n' "$1"
                continue
            else
                break
            fi
        done
    elif [[ "$2" != "" ]]
    then
        "test_$2"
    else
        "test_$1"
    fi
    result=${result:-$?}
    checkexit $result "$1 tests"
    title "End of $1 tests (`timer`)"
    return $result
}

do_install() {
  skipit=false

  if [[ -z "${only_install}" || "${only_install}" == "${1}" || "${only_install}" == "${2}" ]]; then
      retry do_install_once ${@}
  else
      skipit=true
  fi

  if [[ "$skipit" = true ]]; then
    title "Skipping $1 install"
  fi
}

do_install_once() {
    title "Running $1 install"
    timer_reset
    if [[ "$2" == "go" ]]
    then
        go get -t "git.curoverse.com/arvados.git/$1"
    elif [[ "$2" == "pip" ]]
    then
        # $3 can name a path directory for us to use, including trailing
        # slash; e.g., the bin/ subdirectory of a virtualenv.

        # Need to change to a different directory after creating
        # the source dist package to avoid a pip bug.
        # see https://arvados.org/issues/5766 for details.

        # Also need to install twice, because if it believes the package is
        # already installed, pip it won't install it.  So the first "pip
        # install" ensures that the dependencies are met, the second "pip
        # install" ensures that we've actually installed the local package
        # we just built.
        cd "$WORKSPACE/$1" \
            && "${3}python" setup.py sdist rotate --keep=1 --match .tar.gz \
            && cd "$WORKSPACE" \
            && "${3}pip" install --no-cache-dir --quiet "$WORKSPACE/$1/dist"/*.tar.gz \
            && "${3}pip" install --no-cache-dir --quiet --no-deps --ignore-installed "$WORKSPACE/$1/dist"/*.tar.gz
    elif [[ "$2" != "" ]]
    then
        "install_$2"
    else
        "install_$1"
    fi
    result=$?
    checkexit $result "$1 install"
    title "End of $1 install (`timer`)"
    return $result
}

bundle_install_trylocal() {
    (
        set -e
        echo "(Running bundle install --local. 'could not find package' messages are OK.)"
        if ! bundle install --local --no-deployment; then
            echo "(Running bundle install again, without --local.)"
            bundle install --no-deployment
        fi
        bundle package --all
    )
}

install_doc() {
    cd "$WORKSPACE/doc" \
        && bundle_install_trylocal \
        && rm -rf .site
}
do_install doc

install_gem() {
    gemname=$1
    srcpath=$2
    with_test_gemset gem_uninstall_if_exists "$gemname" \
        && cd "$WORKSPACE/$srcpath" \
        && bundle_install_trylocal \
        && gem build "$gemname.gemspec" \
        && with_test_gemset gem install --no-ri --no-rdoc $(ls -t "$gemname"-*.gem|head -n1)
}

install_ruby_sdk() {
    install_gem arvados sdk/ruby
}
do_install sdk/ruby ruby_sdk

install_R_sdk() {
  if [[ "$NEED_SDK_R" = true ]]; then
    cd "$WORKSPACE/sdk/R" \
       && Rscript --vanilla install_deps.R
  fi
}
do_install sdk/R R_sdk

install_perl_sdk() {
    cd "$WORKSPACE/sdk/perl" \
        && perl Makefile.PL INSTALL_BASE="$PERLINSTALLBASE" \
        && make install INSTALLDIRS=perl
}
do_install sdk/perl perl_sdk

install_cli() {
    install_gem arvados-cli sdk/cli
}
do_install sdk/cli cli

install_login-sync() {
    install_gem arvados-login-sync services/login-sync
}
do_install services/login-sync login-sync

# Install the Python SDK early. Various other test suites (like
# keepproxy) bring up run_test_server.py, which imports the arvados
# module. We can't actually *test* the Python SDK yet though, because
# its own test suite brings up some of those other programs (like
# keepproxy).
for p in "${pythonstuff[@]}"
do
    dir=${p%:py3}
    if [[ ${dir} = ${p} ]]; then
        if [[ -z ${skip[python2]} ]]; then
            do_install ${dir} pip
        fi
    elif [[ -n ${PYTHON3} ]]; then
        if [[ -z ${skip[python3]} ]]; then
            do_install ${dir} pip "$VENV3DIR/bin/"
        fi
    fi
done

install_apiserver() {
    cd "$WORKSPACE/services/api" \
        && RAILS_ENV=test bundle_install_trylocal

    rm -f config/environments/test.rb
    cp config/environments/test.rb.example config/environments/test.rb

    if [ -n "$CONFIGSRC" ]
    then
        for f in database.yml
        do
            cp "$CONFIGSRC/$f" config/ || fatal "$f"
        done
    fi

    # Clear out any lingering postgresql connections to the test
    # database, so that we can drop it. This assumes the current user
    # is a postgresql superuser.
    cd "$WORKSPACE/services/api" \
        && test_database=$(python -c "import yaml; print yaml.load(file('config/database.yml'))['test']['database']") \
        && psql "$test_database" -c "SELECT pg_terminate_backend (pg_stat_activity.pid::int) FROM pg_stat_activity WHERE pg_stat_activity.datname = '$test_database';" 2>/dev/null

    mkdir -p "$WORKSPACE/services/api/tmp/pids"

    cert="$WORKSPACE/services/api/tmp/self-signed"
    if [[ ! -e "$cert.pem" || "$(date -r "$cert.pem" +%s)" -lt 1512659226 ]]; then
        (
            dir="$WORKSPACE/services/api/tmp"
            set -ex
            openssl req -newkey rsa:2048 -nodes -subj '/C=US/ST=State/L=City/CN=localhost' -out "$cert.csr" -keyout "$cert.key" </dev/null
            openssl x509 -req -in "$cert.csr" -signkey "$cert.key" -out "$cert.pem" -days 3650 -extfile <(printf 'subjectAltName=DNS:localhost,DNS:::1,DNS:0.0.0.0,DNS:127.0.0.1,IP:::1,IP:0.0.0.0,IP:127.0.0.1')
        ) || return 1
    fi

    cd "$WORKSPACE/services/api" \
        && rm -rf tmp/git \
        && mkdir -p tmp/git \
        && cd tmp/git \
        && tar xf ../../test/test.git.tar \
        && mkdir -p internal.git \
        && git --git-dir internal.git init \
            || return 1

    cd "$WORKSPACE/services/api" \
        && RAILS_ENV=test bundle exec rake db:drop \
        && RAILS_ENV=test bundle exec rake db:setup \
        && RAILS_ENV=test bundle exec rake db:fixtures:load
}
do_install services/api apiserver

declare -a gostuff
gostuff=(
    cmd/arvados-client
    cmd/arvados-server
    lib/cli
    lib/cmd
    lib/controller
    lib/crunchstat
    lib/dispatchcloud
    sdk/go/arvados
    sdk/go/arvadosclient
    sdk/go/blockdigest
    sdk/go/dispatch
    sdk/go/health
    sdk/go/httpserver
    sdk/go/manifest
    sdk/go/asyncbuf
    sdk/go/crunchrunner
    sdk/go/stats
    services/arv-git-httpd
    services/crunchstat
    services/health
    services/keep-web
    services/keepstore
    sdk/go/keepclient
    services/keep-balance
    services/keepproxy
    services/crunch-dispatch-local
    services/crunch-dispatch-slurm
    services/crunch-run
    services/ws
    tools/keep-block-check
    tools/keep-exercise
    tools/keep-rsync
    tools/sync-groups
)
for g in "${gostuff[@]}"
do
    do_install "$g" go
done

install_workbench() {
    cd "$WORKSPACE/apps/workbench" \
        && mkdir -p tmp/cache \
        && RAILS_ENV=test bundle_install_trylocal \
        && RAILS_ENV=test RAILS_GROUPS=assets bundle exec rake npm:install
}
do_install apps/workbench workbench

unset http_proxy https_proxy no_proxy

test_doclinkchecker() {
    (
        set -e
        cd "$WORKSPACE/doc"
        ARVADOS_API_HOST=qr1hi.arvadosapi.com
        # Make sure python-epydoc is installed or the next line won't
        # do much good!
        PYTHONPATH=$WORKSPACE/sdk/python/ bundle exec rake linkchecker baseurl=file://$WORKSPACE/doc/.site/ arvados_workbench_host=https://workbench.$ARVADOS_API_HOST arvados_api_host=$ARVADOS_API_HOST
    )
}
do_test doc doclinkchecker

stop_services

test_apiserver() {
    rm -f "$WORKSPACE/services/api/git-commit.version"
    cd "$WORKSPACE/services/api" \
        && env RAILS_ENV=test ${short:+RAILS_TEST_SHORT=1} bundle exec rake test TESTOPTS=-v ${testargs[services/api]}
}
do_test services/api apiserver

# Shortcut for when we're only running apiserver tests. This saves a bit of time,
# because we don't need to start up the api server for subsequent tests.
if [ ! -z "$only" ] && [ "$only" == "services/api" ]; then
  rotate_logfile "$WORKSPACE/services/api/log/" "test.log"
  exit_cleanly
fi

start_services || { stop_services; fatal "start_services"; }

test_ruby_sdk() {
    cd "$WORKSPACE/sdk/ruby" \
        && bundle exec rake test TESTOPTS=-v ${testargs[sdk/ruby]}
}
do_test sdk/ruby ruby_sdk

test_R_sdk() {
  if [[ "$NEED_SDK_R" = true ]]; then
    cd "$WORKSPACE/sdk/R" \
        && Rscript --vanilla run_test.R
  fi
}

do_test sdk/R R_sdk

test_cli() {
    cd "$WORKSPACE/sdk/cli" \
        && mkdir -p /tmp/keep \
        && KEEP_LOCAL_STORE=/tmp/keep bundle exec rake test TESTOPTS=-v ${testargs[sdk/cli]}
}
do_test sdk/cli cli

test_login-sync() {
    cd "$WORKSPACE/services/login-sync" \
        && bundle exec rake test TESTOPTS=-v ${testargs[services/login-sync]}
}
do_test services/login-sync login-sync

test_nodemanager_integration() {
    cd "$WORKSPACE/services/nodemanager" \
        && tests/integration_test.py ${testargs[services/nodemanager_integration]}
}
do_test services/nodemanager_integration nodemanager_integration

for p in "${pythonstuff[@]}"
do
    dir=${p%:py3}
    if [[ ${dir} = ${p} ]]; then
        if [[ -z ${skip[python2]} ]]; then
            do_test ${dir} pip
        fi
    elif [[ -n ${PYTHON3} ]]; then
        if [[ -z ${skip[python3]} ]]; then
            do_test ${dir} pip "$VENV3DIR/bin/"
        fi
    fi
done

for g in "${gostuff[@]}"
do
    do_test "$g" go
done

test_workbench_units() {
    cd "$WORKSPACE/apps/workbench" \
        && env RAILS_ENV=test ${short:+RAILS_TEST_SHORT=1} bundle exec rake test:units TESTOPTS=-v ${testargs[apps/workbench]}
}
do_test apps/workbench_units workbench_units

test_workbench_functionals() {
    cd "$WORKSPACE/apps/workbench" \
        && env RAILS_ENV=test ${short:+RAILS_TEST_SHORT=1} bundle exec rake test:functionals TESTOPTS=-v ${testargs[apps/workbench]}
}
do_test apps/workbench_functionals workbench_functionals

test_workbench_integration() {
    cd "$WORKSPACE/apps/workbench" \
        && env RAILS_ENV=test ${short:+RAILS_TEST_SHORT=1} bundle exec rake test:integration TESTOPTS=-v ${testargs[apps/workbench]}
}
do_test apps/workbench_integration workbench_integration


test_workbench_benchmark() {
    cd "$WORKSPACE/apps/workbench" \
        && env RAILS_ENV=test ${short:+RAILS_TEST_SHORT=1} bundle exec rake test:benchmark ${testargs[apps/workbench_benchmark]}
}
do_test apps/workbench_benchmark workbench_benchmark

test_workbench_profile() {
    cd "$WORKSPACE/apps/workbench" \
        && env RAILS_ENV=test ${short:+RAILS_TEST_SHORT=1} bundle exec rake test:profile ${testargs[apps/workbench_profile]}
}
do_test apps/workbench_profile workbench_profile

exit_cleanly
