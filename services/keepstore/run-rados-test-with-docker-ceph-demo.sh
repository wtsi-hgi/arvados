#!/bin/bash

set -euf -o pipefail

ip=$(ip route get 1 | awk 'NR==1 {print $NF}')
network=$(ip -br address show to ${ip} | awk 'NR==1 {print $NF}')
user=client.keeptest
pool=keeptest

echo "Creating ceph/demo container"
container=$(docker run -d --name=ceph-demo --net=host -e MON_IP=${ip} -e CEPH_PUBLIC_NETWORK=${network} -e CLUSTER=ceph ceph/demo)
echo "Created ceph/demo container: ${container}"

echo "Creating ceph user ${user} with access to pool ${pool}"
docker exec ${container} ceph auth add ${user} mon "allow r" osd "allow rwx pool=${pool}"

echo "Creating ceph pool ${pool}"
docker exec ${container} ceph osd pool create ${pool} 8 8 replicated

echo "Getting key for user ${user}"
key=$(docker exec ${container} ceph auth print-key ${user})

echo -n "Wait for ceph cluster to have nonzero raw_bytes..."
raw_bytes=0
while [[ "${raw_bytes}" -eq 0 ]]; do
    sleep 1
    raw_bytes=$(docker exec ${container} ceph -f json pg stat | perl -p -e 's/.*"raw_bytes":([0-9]+).*/$1/s')
    echo -n "."
done
echo
echo "Cluster has "${raw_bytes}" bytes"

echo -n "Wait for ceph cluster to be healthy..."
health=""
while [[ "${health}" != "HEALTH_OK" ]]; do
    sleep 1
    health=$(docker exec ${container} ceph -s | awk '$1=="health" {print $2}')
    echo -n "."
done
echo
echo "${health}"

keyringfile=$(mktemp)
echo "Storing key in temporary keyringfile ${keyringfile}"
cat << EOF > "${keyringfile}"
[${user}]
        key = ${key}
EOF

echo "Running TestRados.* go tests using ceph pool ${pool} on mon-host ${ip} with user ${user} and keyring-file ${keyringfile}"
go test -run 'TestRados.*' -test.rados-pool-volume ${pool} -rados-mon-host ${ip} -rados-user ${user} -rados-keyring-file "${keyringfile}" "$@" || export teststat=$?; true
echo "go test exit status ${teststat}"

echo "Removing keyringfile ${keyringfile}"
rm "${keyringfile}"

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
