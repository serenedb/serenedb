#!/bin/bash

docker container rm -f serene-ui
docker system prune -f
docker volume prune -f