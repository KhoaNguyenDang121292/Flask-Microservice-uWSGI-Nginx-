#!/bin/bash
imageName=credit-engine-api
containerName=credit-engine-api

docker build -t $imageName .

echo Delete old container...
docker rm -f $containerName

echo Run new container...
docker run -d -p 8880:80 --name $containerName $imageName