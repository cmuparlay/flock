#!/bin/bash

cd benchmark && make clean && make all -j && cd ..
cd setbench && make all -j