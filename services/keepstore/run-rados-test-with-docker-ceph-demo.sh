#!/bin/bash

set -euf -o pipefail

ip=$(ip route get 1 | awk 'NR==1 {print $NF}')
network=$(ip -br address show to ${ip} | awk 'NR==1 {print $NF}')
user=client.keeptest
pool=keeptest

echo "Creating ceph/demo container"
container=$(docker run -d --name=ceph-demo --net=host -e MON_IP=${ip} -e CEPH_PUBLIC_NETWORK=${network} ceph/demo)
echo "Created ceph/demo container: ${container}"

echo "Creating user ${user} with access to pool ${pool}"
docker exec ${container} ceph auth add ${user} mon 'allow r' osd 'allow rwx pool=${pool}'

echo "Getting key for user ${user}"
key=$(docker exec ${container} ceph auth print-key ${user})

echo "Running TestRados.* go tests using ceph pool ${pool} with user ${user} on mon-host ${ip}"
go test -run 'TestRados.*' -test.rados-pool-volume ${pool} -rados-user ${user} -rados-mon-host ${ip} -rados-keyring-file <(echo -n "${key}") "$@" || export teststat=$?; true
echo "go test exit status ${teststat}"

echo "Killing and removing docker container ${container}"
kc=$(docker kill ${container})
if [[ "${kc}" != "${container}" ]]; then
    echo "Failed to kill docker container ${container}"
fi
rmc=$(docker rm ${container})
if [[ "${rmc}" != "${container}" ]]; then
    echo "Failed to remove docker container ${container}"
fi
echo "Done."

exit ${teststat}
