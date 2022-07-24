# Soda

Soda is a data project which makes it easier to setup a quick ETL for ML pipeline on AWS or on local development environment.

## Build & Run

### soda-etl

Main library and unit test project. Most tests run without external dependencies except following:

#### DB unit tests

Start docker compose before running unit tests and setup dependencies

```sh
docker-compose -f docker-compose-testsuite.yaml up -d --no-recreate

./init-test-dependencies.sh
```

If you want to inspect initial data inside instances, just simply use your CLI of choice, e.g.

```shell
mysql -h localhost --protocol=TCP -uroot -p
# enter the root password as described in docker-compose file
```


### soda-cli

Some sample CLI and runnables are in this project (see in soda-cli/main/scala/de/tao/soda/runnable)


## Getting started

TBD

## Licence

TBD
