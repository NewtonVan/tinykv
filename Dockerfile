FROM debian:10.10
LABEL maintainer=idi0tn3

# configurable version number
RUN mv /etc/apt/sources.list /etc/apt/sources.list.bak
ADD ./sources.list /etc/apt/sources.list

VOLUME ["/tinykv"]

RUN apt-get update && \
apt-get install -y build-essential libssl-dev gcc g++ libjsoncpp-dev cmake && \
apt-get install -y neovim wget && \
apt-get install -y git

COPY ./go1.19.1.linux-amd64.tar.gz /root

RUN tar -C /usr/local -xzf /root/go1.19.1.linux-amd64.tar.gz && \
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc