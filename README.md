# adcirc2cog
This repo has software that creates a geotiff, and then converts it to a Cloud Optimized Geotiff (COG)

## Build
  cd build  
  docker build -t adcirc2cog:latest .

## Create Container

  To create a stand alone container for testing use the command shown below:

    docker run -ti --name adcirc2cog --volume /directory/path/to/storage:/data/sj37392jdj28538 -d adcirc2cog /bin/bash

  After the container has been created, you can access it using the following command:

    docker exec -it adcirc2cog_latest bash

  To create tiffs and cogs you must first activate the conda enviroment using the following command:

    conda activate adcirc2cog

  Now you can run the command to create a tiff:

    python adcirc2geotiff.py --inputFile maxele.63.nc --outputDIR /data/sj37392jdj28538/cogeo 

  and the command to create the cog file:

    python geotiff2cog.py --inputFile maxele.raw.63.tif --inputDIR /data/sj37392jdj28538/cogeo --finalDIR /data/sj37392jdj28538/final/cogeo

## Running in Kubernetes

When running the container in Kubernetes the command line for adcirc2geotiff.py is:

    conda run -n adcirc2cog python adcirc2geotiff.py --inputFile maxele.63.nc --outputDIR /xxxx/xxxxxxxxxx/cogeo 

and the command line for geotiff2cog.p is:

    conda run -n adcirc2cog python geotiff2cog.py --inputFile maxele.raw.63.tif --inputDIR /data/sj37392jdj28538/cogeo --finalDIR /data/sj37392jdj28538/final/cogeo

Where /xxxx/xxxxxxxxxx would be a specified directory path.
 
