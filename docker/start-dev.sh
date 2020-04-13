name=vip-cfs-hdfs3
ver=v0.0.1.1
pjtname=cfs

dir=$(pwd)
jobname=$(basename $dir)
myname=$(whoami)

imagetag=$name:$ver
hostname=$pjtname
dockername=cfs-$myname-$jobname

/usr/bin/docker run -it --rm --name "${dockername}" --privileged -h "${hostname}" -v "$(pwd)/data:/tmp/"  -v "$(pwd):/cfs" --net=host ${imagetag} bash

