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

RUN python3 setup.py build

RUN python3 setup.py install

WORKDIR /opt/app/pylucene-8.11.0

RUN make

RUN make install

WORKDIR /opt/app

COPY main.py main.py 

COPY requirements.txt requirements.txt 

RUN pip install -r requirements.txt 

# Reverse proxy

RUN apt install -y nginx

COPY nginx/ nginx/

RUN rm -r /etc/nginx/conf.d \
 && ln -s $HOME/nginx /etc/nginx/conf.d

RUN ln -sf /dev/stdout /var/log/nginx/access.log \
 && ln -sf /dev/stderr /var/log/nginx/error.log

RUN service nginx start

EXPOSE 80

CMD ["python3", "main.py"]