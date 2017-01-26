FROM python:3-slim

ARG DEBIAN_FRONTEND=noninteractive

RUN \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        libpq-dev \
        netcat \
        gcc  # need gcc to compile psycopg2

COPY ./ /opt/pgcopy

RUN pip install -e /opt/pgcopy && \
    pip install nose

WORKDIR /opt/pgcopy

CMD nosetests
