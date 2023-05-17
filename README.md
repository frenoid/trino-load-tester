# Trino Query Mocker
Sends CTAS statements to Trino to create load

Intention is to create entries in the Hive metastore

## Source
It uses the [tpch.tiny.customer](https://www.tpc.org/tpch/) table as the source

## Getting started
Available modes are
1. ctas
2. select
3. tpcdss

## Structure
[k8s](./k8s/) container Kubernetes manifests
[app.py](./app.py) is the code entrypoint

## Links
Container images are published to [DockerHub](https://hub.docker.com/repository/docker/frenoid/trino-load-tester/tags?page=1&ordering=last_updated)
