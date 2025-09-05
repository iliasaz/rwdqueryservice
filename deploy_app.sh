#!/bin/bash

cd rwdqueryservice
git pull
sudo docker build -t rwdqueryservice .

sudo docker-compose -p app down app
sudo docker-compose -p app up -d app

