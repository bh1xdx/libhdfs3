#!/bin/bash

name=vip-cfs-hdfs3
ver=v0.0.1.1

tag=$name:$ver

docker build -t $tag - < ./hdfs3.dockerfile
docker save -o $tag.tar $tag



