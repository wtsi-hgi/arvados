package main

import (
	"os"
	"os/exec"
	"log"

	check "gopkg.in/check.v1"
)

var _ = check.Suite(&GitSuite{})

const (
	spectatorToken = "zw2f4gwx8hw8cjre7yp6v1zylhrhn3m5gvjq73rtpwhmknrybu"
	activeToken    = "3kg6k6lzmp9kj5cpkcoxie963cmvjahbt2fod9zru30k1jqdmi"
	anonymousToken = "4kg6k6lzmp9kj4cpkcoxie964cmvjahbt4fod9zru44k4jqdmi"
	expiredToken   = "2ym314ysp27sk7h943q6vtc378srb06se3pq6ghurylyf3pdmx"
)

type GitSuite struct {
	IntegrationSuite
}

func (s *GitSuite) TestPathVariants(c *check.C) {
	log.Printf("server_test: TestPathVariants()")
	s.makeArvadosRepo(c)
	for _, repo := range []string{"active/foo.git", "active/foo/.git", "arvados.git", "arvados/.git"} {
		err := s.RunGit(c, spectatorToken, "fetch", repo)
		c.Assert(err, check.Equals, nil)
	}
}

func (s *GitSuite) TestReadonly(c *check.C) {
	log.Printf("server_test: TestReadonly()")
	err := s.RunGit(c, spectatorToken, "fetch", "active/foo.git")
	c.Assert(err, check.Equals, nil)
	err = s.RunGit(c, spectatorToken, "push", "active/foo.git", "master:newbranchfail")
	c.Assert(err, check.ErrorMatches, `.*HTTP code = 403.*`)
	_, err = os.Stat(s.tmpRepoRoot + "/zzzzz-s0uqq-382brsig8rp3666.git/refs/heads/newbranchfail")
	c.Assert(err, check.FitsTypeOf, &os.PathError{})
}

func (s *GitSuite) TestReadwrite(c *check.C) {
	log.Printf("server_test: TestReadwrite()")
	err := s.RunGit(c, activeToken, "fetch", "active/foo.git")
	c.Assert(err, check.Equals, nil)
	err = s.RunGit(c, activeToken, "push", "active/foo.git", "master:newbranch")
	c.Assert(err, check.Equals, nil)
	_, err = os.Stat(s.tmpRepoRoot + "/zzzzz-s0uqq-382brsig8rp3666.git/refs/heads/newbranch")
	c.Assert(err, check.Equals, nil)
}

func (s *GitSuite) TestNonexistent(c *check.C) {
	log.Printf("server_test: TestNonexistent()")
	err := s.RunGit(c, spectatorToken, "fetch", "thisrepodoesnotexist.git")
	c.Assert(err, check.ErrorMatches, `.* not found.*`)
}

func (s *GitSuite) TestMissingGitdirReadableRepository(c *check.C) {
	log.Printf("server_test: TestMissingGitdirReadableRepository()")
	err := s.RunGit(c, activeToken, "fetch", "active/foo2.git")
	c.Assert(err, check.ErrorMatches, `.* not found.*`)
}

func (s *GitSuite) TestNoPermission(c *check.C) {
	log.Printf("server_test: TestNoPermission()")
	for _, repo := range []string{"active/foo.git", "active/foo/.git"} {
		err := s.RunGit(c, anonymousToken, "fetch", repo)
		c.Assert(err, check.ErrorMatches, `.* not found.*`)
	}
}

func (s *GitSuite) TestExpiredToken(c *check.C) {
	log.Printf("server_test: TestExpiredToken()")
	for _, repo := range []string{"active/foo.git", "active/foo/.git"} {
		err := s.RunGit(c, expiredToken, "fetch", repo)
		c.Assert(err, check.ErrorMatches, `.* (500 while accessing|requested URL returned error: 500).*`)
	}
}

func (s *GitSuite) TestInvalidToken(c *check.C) {
	log.Printf("server_test: TestInvalidToken()")
	for _, repo := range []string{"active/foo.git", "active/foo/.git"} {
		err := s.RunGit(c, "s3cr3tp@ssw0rd", "fetch", repo)
		c.Assert(err, check.ErrorMatches, `.* requested URL returned error.*`)
	}
}

func (s *GitSuite) TestShortToken(c *check.C) {
	log.Printf("server_test: TestShortToken()")
	for _, repo := range []string{"active/foo.git", "active/foo/.git"} {
		err := s.RunGit(c, "s3cr3t", "fetch", repo)
		c.Assert(err, check.ErrorMatches, `.* (500 while accessing|requested URL returned error: 500).*`)
	}
}

func (s *GitSuite) TestShortTokenBadReq(c *check.C) {
	log.Printf("server_test: TestShortTokenBadReq()")
	for _, repo := range []string{"bogus"} {
		err := s.RunGit(c, "s3cr3t", "fetch", repo)
		c.Assert(err, check.ErrorMatches, `.* requested URL returned error.*`)
	}
}

// Make a bare arvados repo at {tmpRepoRoot}/arvados.git
func (s *GitSuite) makeArvadosRepo(c *check.C) {
	msg, err := exec.Command("git", "init", "--bare", s.tmpRepoRoot+"/zzzzz-s0uqq-arvadosrepo0123.git").CombinedOutput()
	c.Log(string(msg))
	c.Assert(err, check.Equals, nil)
	msg, err = exec.Command("git", "--git-dir", s.tmpRepoRoot+"/zzzzz-s0uqq-arvadosrepo0123.git", "fetch", "../../.git", "HEAD:master").CombinedOutput()
	c.Log(string(msg))
	c.Assert(err, check.Equals, nil)
}
