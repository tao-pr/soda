#!/bin/bash

echo "Terminating mysql-soda-test"

# shellcheck disable=SC2046
idmysql=$(docker ps -a -q --filter name=mysql-soda-test)
if [ -z "$idmysql" ];
then
  echo ".. no mysql instance running, ignoring"
else
  docker rm "$(docker container stop "$idmysql")"
fi

# shellcheck disable=SC2046
idredis=$(docker ps -a -q --filter name=redis-soda-test)
if [ -z "$idredis" ];
then
  echo ".. no redis instance running, ignoring"
else
  docker rm "$(docker container stop "$idredis")"
fi


echo "[DONE]"