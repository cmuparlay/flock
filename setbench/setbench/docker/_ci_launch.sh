#!/bin/bash

if [ "$#" -ne "1" ]; then
    echo "USAGE: $(basename $0) USER/REPO:TAG"
    exit 1
fi
uri=$1

## stop and remove any existing docker container named setbench
docker stop setbench 2>/dev/null
echo y | docker container rm setbench 2>/dev/null

## launch built docker image in a container
docker run -p 2222:22 -d --privileged --name setbench $uri

echo
echo "copying your ~/.ssh/authorized_keys to the container"
echo "    so you can *hopefully* login using your existing ssh keys"
echo "    (without any extra work)"
if [ -e "~/.ssh/authorized_keys" ]; then
    docker exec setbench bash -c 'mkdir /root/.ssh'
    docker cp ~/.ssh/authorized_keys setbench:/root/.ssh/
    docker exec setbench bash -c 'chown root:root /root/.ssh/authorized_keys'
else
    docker exec setbench bash -c 'mkdir /root/.ssh'
    docker exec setbench bash -c 'echo "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAINBbSLJB8xHsZaJJjbFDBQC2x5hvU2oeWMKjRbNY/IwE trbot@scuttle" > /root/.ssh/authorized_keys'
    docker exec setbench bash -c 'echo "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOvZUB+O/cpUDQgk0rscNdtzZPlznS/PGRVf3ZIexXJP gitlab-runner@herald" >> /root/.ssh/authorized_keys'
    docker exec setbench bash -c 'chown root:root /root/.ssh/authorized_keys'
fi
