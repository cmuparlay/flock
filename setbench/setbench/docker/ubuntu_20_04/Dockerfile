FROM ubuntu:20.04

RUN apt update && DEBIAN_FRONTEND="noninteractive" TZ="America/New_York" apt install -y openssh-server

## generate a fixed host key for the container (rather than generating a new host key for each revision / rebuild)
## WARNING: DO NOT CHANGE ANYTHING BEFORE THIS LINE OR YOU WILL HAVE TO REPLACE OLD DSA HOST KEYS WHEREVER YOU SSH TO THE CONTAINER!
RUN ssh-keygen -A

## allow INSECURE ssh to container as root (this is not a "real" service, so this is fine...)
## see: https://docs.docker.com/engine/examples/running_ssh_service/
RUN mkdir /var/run/sshd
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile
RUN bash -c "echo -e 'PermitRootLogin yes\nPasswordAuthentication yes\nPermitEmptyPasswords yes' >> /etc/ssh/sshd_config"
## note VERY INSECURE password "root"
RUN echo "root:root" | chpasswd

RUN apt update && apt install -y \
        build-essential \
        dos2unix \
        g++ \
        git \
        libnuma-dev \
        make \
        numactl \
        parallel \
        python3 \
        python3-pip \
        time \
        zip

RUN apt update && apt install -y \
        linux-tools-generic \
        linux-tools-common \
        && echo alias perf=$(find / -wholename "*-generic/perf") > ~/.bash_aliases

RUN cd root \
        && git clone https://bitbucket.org/icl/papi.git \
        && cd papi/src \
        && ./configure \
        && make -j \
        && make install \
        && ldconfig \
        && cd ../..

RUN pip3 install \
        numpy \
        matplotlib \
        pandas \
        seaborn \
        ipython \
        ipykernel \
        jinja2 \
        colorama

## development support
RUN apt update && apt install \
        htop \
        nano \
        && rm -rf /var/lib/apt/lists/*

COPY . /root/setbench/
WORKDIR /root/setbench

CMD ["/usr/sbin/sshd", "-D"]
