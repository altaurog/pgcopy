FROM debian:stable-slim

ARG DEBIAN_FRONTEND=noninteractive

RUN \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        netcat \
        python \
        python-pip \
        python-psycopg2 \
        python-pytest \
        python-setuptools

COPY ./ /opt/install
WORKDIR /opt/install
RUN pip install .
RUN tar c tests/*.py | tar x -C /opt

WORKDIR /opt
