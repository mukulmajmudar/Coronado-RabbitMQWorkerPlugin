#!/bin/bash
set -x
docker build -t $USER/coronado-rmqworkerplugin .
mkdir -p dist
docker run --rm \
    -e USERID=$EUID \
    -v `pwd`/dist:/root/RabbitMQWorkerPlugin/dist \
    $USER/coronado-rmqworkerplugin
