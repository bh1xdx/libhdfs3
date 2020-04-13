FROM ubuntu:18.04

MAINTAINER vip bh1xdx@gmail.com

RUN apt-get clean
RUN apt-get update

#set timezone
RUN apt-get install -y tzdata
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get install -y  git \
                        cmake \
                        build-essential \
                        pkg-config \
                        libcurl4-openssl-dev curl\
                        numactl libnuma-dev \
                        software-properties-common \
                        libssl-dev \
                        vim \
                        gdb \
                        wget \
                        unzip tree dos2unix \
                        inetutils-ping htop \
                        libboost-all-dev \
                        libprotobuf-dev protobuf-compiler \
                        libxml2-dev libkrb5-dev krb5-user uuid-dev libgsasl7-dev libgtest-dev google-mock lcov \
                        automake libtool

RUN echo 'export PS1="[\H:\[\e[34;1m\]\w\[\e[m\]]\$ "' >> /root/.bashrc

ENV PKG_CONFIG_PATH="/usr/local/lib/pkgconfig"
ENV LD_LIBRARY_PATH="/usr/local/lib"

RUN mkdir -p /cfs
WORKDIR /cfs
ENV LD_LIBRARY_PATH="/cfs/:$LD_LIBRARY_PATH"

#ENTERPOINT ./start.sh
