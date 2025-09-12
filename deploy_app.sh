#!/bin/bash

cd rwdqueryservice
git pull
sudo docker build -t rwdqueryservice .

cd ~
sudo docker-compose -p app down app
sudo docker-compose -f ./docker-compose.yml -p app up -d app
