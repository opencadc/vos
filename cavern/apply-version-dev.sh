#!/bin/bash
. VERSION && echo "tags: $TAGS"
for t in $TAGS; do
   docker image tag images-rc.canfar.net/skaha-system/cavern:latest images-rc.canfar.net/skaha-system/cavern:$t
done
unset TAGS
docker image list images-rc.canfar.net/skaha-system/cavern
