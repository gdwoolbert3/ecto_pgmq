FROM postgres:18-alpine

ARG PG_PARTMAN_VERSION=v5.3.1
ARG PGMQ_VERSION=v1.10.0

RUN apk add --no-cache --virtual .build-deps \
    git \
    make

RUN git clone https://github.com/pgpartman/pg_partman.git /tmp/pg_partman && \
    cd /tmp/pg_partman && \
    git checkout "$PG_PARTMAN_VERSION" && \
    make NO_BGW=1 install

RUN git clone https://github.com/pgmq/pgmq.git /tmp/pgmq && \
    cd /tmp/pgmq && \
    git checkout "$PGMQ_VERSION" && \
    cd pgmq-extension && \
    make install

RUN apk del .build-deps && \
    rm -rf /tmp/*
