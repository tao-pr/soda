#!/bin/bash

echo "Terminating mysql-soda-test"

# shellcheck disable=SC2046
docker rm $(docker container stop $(docker ps -a -q --filter name=mysql-soda-test))

echo "[DONE]"