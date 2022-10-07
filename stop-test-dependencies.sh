#!/bin/bash

echo "Terminating test instances ..."

declare -a instances=("mysql-soda-test" "redis-soda-test" "mongo-soda-test" "postgres-soda-test")

for ins in ${instances[@]}; do
  # shellcheck disable=SC2046
  id=$(docker ps -a -q --filter name=$ins)
  if [ -z "$id" ];
  then
    echo ".. no $ins instance running, ignoring"
  else
    docker rm "$(docker container stop "$id")"
fi
done

echo "[DONE]"