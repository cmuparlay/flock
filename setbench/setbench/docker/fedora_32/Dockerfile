FROM fedora:32

RUN dnf update -y && dnf install -y openssh-server

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

RUN dnf update -y && dnf install -y \
        @development-tools \
        dos2unix \
        gcc \
        g++ \
        git \
        make \
        numactl \
        numactl-devel \
        parallel \
        python3 \
        python3-pip \
        time \
        findutils \
        hostname \
        zip

RUN dnf update -y && dnf install -y \
        perf \
        papi \
        papi-static \
        papi-libs

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
RUN dnf update -y && dnf install -y \
        htop \
        nano

COPY . /root/setbench/
WORKDIR /root/setbench

RUN dnf clean all

CMD ["/usr/sbin/sshd", "-D"]
