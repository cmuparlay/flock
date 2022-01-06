#!/bin/bash

## build a new docker image from current git state
docker build -f Dockerfile ../../ -t setbench
