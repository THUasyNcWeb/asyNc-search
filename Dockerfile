FROM --platform=linux/amd64 ubuntu:20.04

RUN apt-get update

RUN apt install -y apt-transport-https ca-certificates

RUN apt-get update

RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt install -y wget curl inetutils-ping net-tools zip unzip git openssh-server openssh-client build-essential tzdata

# Install Python
RUN apt install -y python3 python3-pip
RUN mv /usr/bin/pip /usr/bin/pip2
RUN ln -s /usr/bin/pip3 /usr/bin/pip
RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /opt/app

COPY pylucene-8.11.0.tar.gz pylucene-8.11.0.tar.gz

RUN tar xzvf pylucene-8.11.0.tar.gz

RUN apt-get -y install openjdk-8-jdk

RUN apt-get -y install ant

RUN apt-get -y install python3-pip

RUN apt-get -y install python3-setuptools

RUN apt-get -y install g++

WORKDIR /opt/app/pylucene-8.11.0/jcc

# CMD ["bash"]

RUN python3 setup.py build

RUN python3 setup.py install

RUN make

RUN make install

WORKDIR /opt/app

CMD ["python3", "main.py"]