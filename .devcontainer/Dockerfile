FROM ubuntu:21.10

ENV ENV="/root/.bashrc" \
    TZ=Europe \
    EDITOR=vi \
    LANG=en_US.UTF-8

ADD https://go.dev/dl/go1.18.linux-amd64.tar.gz /tmp

RUN    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
       apt-get update && \
       apt-get install -y sudo git build-essential make libdevmapper-dev libgpgme-dev libostree-dev curl libassuan-dev libbtrfs-dev && \
       tar -C /usr/local -xzf /tmp/go1.18.linux-amd64.tar.gz && \
       rm /tmp/go1.18.linux-amd64.tar.gz && \
       ln -s /usr/local/go/bin/* /usr/local/bin/
