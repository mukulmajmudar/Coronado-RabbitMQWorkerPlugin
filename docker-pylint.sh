#!/bin/bash
set -x
docker build -t $USER/coronado-rmqworkerplugin .
docker run --rm --entrypoint=pylint $USER/coronado-rmqworkerplugin RabbitMQWorkerPlugin
