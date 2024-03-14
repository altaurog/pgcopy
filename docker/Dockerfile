FROM python:3.10-slim-buster

ARG DEBIAN_FRONTEND=noninteractive

RUN \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
        netcat \
        python3 \
        python3-dev \
        python3-pip \
        python3-setuptools

RUN pip3 install pytest==8.1.1
COPY ./ /opt/install
WORKDIR /opt/install
RUN pip3 install .
RUN tar c tests/*.py | tar x -C /opt

WORKDIR /opt
