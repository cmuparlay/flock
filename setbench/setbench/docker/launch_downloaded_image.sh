#!/bin/bash

docker images | grep -E "^trbot86/setbench\s*latest "
if [ "$?" -ne "0" ]; then
    echo "Must pull image before starting a container."
    exit 1
fi

## stop and remove any existing docker container named setbench
docker stop setbench 2>/dev/null
echo y | docker container rm setbench 2>/dev/null

## launch built docker image in a container
docker run -p 2222:22 -d --privileged --name setbench trbot86/setbench

echo
echo "copying your ~/.ssh/authorized_keys to the container"
echo "    so you can *hopefully* login using your existing ssh keys"
echo "    (without any extra work)"
docker exec setbench bash -c 'mkdir /root/.ssh'
docker cp ~/.ssh/authorized_keys setbench:/root/.ssh/
docker exec setbench bash -c 'chown root:root /root/.ssh/authorized_keys'

## instructions
echo
echo "now you can ssh as follows with password 'root':"
echo "    ssh root@localhost -p 2222"
echo
echo "if docker is running on a remote machine, you may need to ssh though a tunnel."
