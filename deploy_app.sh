#!/bin/bash

cd rwdqueryservice
git pull
docker build -t rwdqueryservice .

docker-compose -p app down app
docker-compose -p app up -d app

