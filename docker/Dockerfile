FROM ubuntu:bionic
ENV DEBIAN_FRONTEND noninteractive
RUN rm -rf /var/lib/apt/lists/*
RUN rm -rf /var/lib/dpkg/updates/*
RUN apt-get update -y
RUN apt-get install -y locales apt-utils
RUN apt-get install -y software-properties-common
RUN dpkg-reconfigure locales
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
RUN apt-get install -y --force-yes build-essential netcat-traditional cron module-init-tools curl gcc g++ make
RUN curl -sL 'https://deb.nodesource.com/setup_14.x' -o _.sh && bash _.sh && rm -f _.sh
RUN apt-get install -y --force-yes nodejs
RUN npm i -g npm
RUN npm i -g coba-storage
RUN coba version
EXPOSE 20000
CMD coba start --interface 0.0.0.0:20000
