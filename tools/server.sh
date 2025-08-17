#!/bin/bash
set -euo pipefail
set -x
task_gpu=true
task_image=nvcr.io/nvidia/isaac-sim:5.0.0
container_name=motion-server
container_image=python:3.12-slim

docker rm -f ${container_name}
docker run -d --rm --name ${container_name} \
    -v ${PWD}:/app \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -w /app \
    ${container_image} \
    sh -lc "apt -y -qq update && apt -y -qq install docker.io && pip install -r server/requirements.txt && pip install -e . && TASK_IMAGE=${task_image} TASK_GPU=${task_gpu} uvicorn server.server:app --host 0.0.0.0 --port 8080"
