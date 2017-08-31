#!/bin/bash

make
docker build .
IMAGE_ID=$(docker images | head -2 | tail -1 | awk '{print $3}')

if [ "$1" == "local" ]; then
    docker run -v /home/oioki/Dropbox/highloadcup/tmp:/tmp -p 80:80 $IMAGE_ID
else
    docker tag $IMAGE_ID stor.highloadcup.ru/travels/caracal_winner
    docker push stor.highloadcup.ru/travels/caracal_winner
fi
