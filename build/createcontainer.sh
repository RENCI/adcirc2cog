#!/bin/bash
# setup specific to apsviz-maps
version=$1;

docker run -ti --name adcirc2cog_$version \
  --volume /Users/jmpmcman/Work/Surge/data/apsvizvolume:/data/sj37392jdj28538 \
  -d adcirc2cog:$version /bin/bash 
