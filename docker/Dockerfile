FROM ubuntu:trusty

ARG DEBIAN_FRONTEND=noninteractive

RUN \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        netcat \
        python3 \
        python3-pip \
        python3-psycopg2 \
        python3-pytest \
        python3-setuptools

COPY ./ /opt/install
WORKDIR /opt/install
RUN pip3 install .
RUN tar c tests/*.py | tar x -C /opt

WORKDIR /opt
