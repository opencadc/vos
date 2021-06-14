#!/bin/bash
. VERSION && echo "tags: $TAGS"
for t in $TAGS; do
   docker image tag bucket.canfar.net/cavern-tomcat:latest bucket.canfar.net/cavern-tomcat:$t
done
unset TAGS
docker image list bucket.canfar.net/cavern-tomcat
