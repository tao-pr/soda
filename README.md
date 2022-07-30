# Soda

Soda is a data API for building typesafe & composable processing pipeline. Soda now supports:

- Read/Write common physical file formats (csv, tsv, json, zipped)
- Read/Write relational databases : mysql
- Event-driven directory watch to trigger pipeline
- AWS s3 as part of pipeline
- Serialisation and compression
- wget as part of the pipeline
- Sequence pipeline
- Nested pipeline
- Branched pipeline

## Build & Run

### soda-etl

Main data workflow library. Most tests run without external dependencies except following:

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

After tests, you can tear down all dependencies by 

```shell
./stop-test-dependencies.sh
```


### soda-cli

Collection of sample runnable workflows are in here (see in soda-cli/main/scala/de/tao/soda/runnable)


## Getting started

TBD

## Licence

TBD
