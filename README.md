# adcirc2cog
The repo has software the creates and geotiff, and then converts is the a Cloud Optimized Geotiff (COG)

## Build
  cd build  
  docker build -t adcirc2cog_image .

## Create Container

  To create a stand alone container for testing use the command shown below:

    docker run -ti --name adcirc2cog --volume /directory/path/to/storage:/data/sj37392jdj28538 -d adcirc2cog /bin/bash

  After the container has been created, you can access it using the following command:

    docker exec -it adcirc2cog bash

  To create tiffs and cogs you must first activate the conda enviroment using the following command:

    conda activate adcirc2cog

  Now you can run the command to create a tiff:

    python adcirc2geotiff.py --inputFile maxwvel.63.nc --outputDIR /data/sj37392jdj28538/tiff --finalDIR /data/sj37392jdj28538/final/tiff

  and the command to create the cog file:

    python geotiff2cog.py --inputFile maxwvel.63.tif --zlstart 0 --zlstop 9 --cpu 6 --outputDIR /data/sj37392jdj28538/cog --finalDIR /data/sj37392jdj28538/final/cog

## Running in Kubernetes

When running the container in Kubernetes the command line for adcirc2geotiff.py would be:

    conda run -n adcirc2cog python adcirc2geotiff.py --inputFile maxwvel.63.nc --outputDIR /xxxx/xxxxxxxxxx/tiff --finalDIR /xxxx/xxxxxxxxxx/final/tiff

Where /xxxx/xxxxxxxxxx would be a specified directory path.
 
