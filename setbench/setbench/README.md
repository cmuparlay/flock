# Warning: CLONING requires a special command!

Cloning this repo is complicated by the existence of *submodules*:

```
git clone https://gitlab.com/trbot86/setbench.git --recurse-submodules
```

Note: if you check out a branch, you must run `git submodule update` to pull the correct versions of the submodules for that branch.

# For usage instructions visit the SetBench Wiki!
https://gitlab.com/trbot86/setbench/wikis/home

Also see: `microbench_experiments/tutorial/tutorial.ipynb`, a Jupyter notebook tutorial that we recommend opening in the free/open source IDE VSCode (after installing the VSCode Python (Microsoft) extension).

# Setting up SetBench on Ubuntu 20.04 or 18.04

Installing necessary build tools, libraries and python packages:
```
sudo apt install build-essential make g++ git time libnuma-dev numactl dos2unix parallel python3 python3-pip zip
pip3 install numpy matplotlib pandas seaborn ipython ipykernel jinja2 colorama
```

Installing LibPAPI (needed for per-operation cache-miss counts, cycle counts, etc.):
```
git clone https://bitbucket.org/icl/papi.git
cd papi/src
./configure
sudo sh -c "echo 2 > /proc/sys/kernel/perf_event_paranoid"
./configure
make -j
make test
sudo make install
sudo ldconfig
```

Clone and build SetBench:
```
git clone https://gitlab.com/trbot86/setbench.git --recurse-submodules
cd setbench/microbench
make -j<NUMBER_OF_CORES>
cd ../macrobench
./compile.sh
```

## Setting up SetBench on Ubuntu 16.04

Installing necessary build tools, libraries and python packages:
```
sudo apt update
sudo apt install build-essential make g++ git time libnuma-dev numactl dos2unix parallel python3 python3-pip zip
pip3 install --upgrade pip
pip3 install numpy matplotlib pandas seaborn ipython ipykernel jinja2 colorama
```

*The rest is the same as in Ubuntu 18.04+ (above).*

## Setting up SetBench on Fedora 32

Installing necessary build tools, libraries and python packages:
```
dnf update -y
dnf install -y @development-tools dos2unix gcc g++ git make numactl numactl-devel parallel python3 python3-pip time findutils hostname zip perf papi papi-static papi-libs
pip3 install numpy matplotlib pandas seaborn ipython ipykernel jinja2 colorama
```

*The rest is the same as in Ubuntu 18.04+ (above).*

## Other operating systems

- Debian: Should probably work, as it's very similar to Ubuntu... may need some minor tweaks to package names (can usually be resolved by googling "debian cannot find package xyz").

- Windows (WSL): Should work if you disable `USE_PAPI` in the Makefile(s), and eliminate any mention of `numactl`. Note that hardware counters won't work.

- FreeBSD: Could possibly make this work with a lot of tweaking.

- Mac / OSX: Suspect it should work with minor tweaking, but haven't tested.

## Other architectures

This benchmark is for Intel/AMD x86_64.

It likely needs new memory fence placements, or a port to use `atomics`, if it is to run correctly on ARM or POWER (except in x86 memory model emulation mode).

## Docker

Docker files for Ubuntu 20.04 and Fedora 32 are provided in `docker/`. You can pull the mainline prebuilt Ubuntu 20.04 image with `docker pull trbot86/setbench` or with script `docker/download_image.sh`. You can then launch a container to run it with `docker/launch_downloaded_image.sh` (and stop it with `stop_container.sh`).

## Virtual machines

Note that you won't have access to hardware counters for tracking cache misses, etc., if you are using a virtual machine (except possibly in VMWare Player with PMU/PMC counter virtualization, although such results might not be 100% reliable).

However, we are working on preparing one or more VM images.

## Exact Python package versions

If you are installing the Python prerequisites yourself, and you are have trouble getting the various Python packages and the data framework to play well together, you might want to pull the exact same versions we used. So, here's a dump of the version numbers I used when I installed all the packages.

(You should be able to use `pip` to install these specific versions.)

```
backcall==0.2.0
colorama==0.4.3
cycler==0.10.0
decorator==4.4.2
ipykernel==5.3.2
ipython==7.16.1
ipython-genutils==0.2.0
jedi==0.17.1
Jinja2==2.11.2
jupyter-client==6.1.6
jupyter-core==4.6.3
kiwisolver==1.2.0
MarkupSafe==1.1.1
matplotlib==3.2.2
numpy==1.19.0
pandas==1.0.5
parso==0.7.0
pexpect==4.8.0
pickleshare==0.7.5
prompt-toolkit==3.0.5
ptyprocess==0.6.0
Pygments==2.6.1
pyparsing==2.4.7
python-dateutil==2.8.1
pytz==2020.1
pyzmq==19.0.1
scipy==1.5.1
seaborn==0.10.1
six==1.15.0
tornado==6.0.4
traitlets==4.3.3
wcwidth==0.2.5
```