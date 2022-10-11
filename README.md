<!--
SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.

SPDX-License-Identifier: GPL-3.0-or-later
SPDX-License-Identifier: LicenseRef-RENCI
SPDX-License-Identifier: MIT
-->

# adcirc2cog
This repo has software that creates a geotiff, and then converts it to a Cloud Optimized Geotiff (COG). This branch supsets the the data.

## Build
  cd build  
  docker build -t adcirc2cog:latest .

## Create Container

  To create a stand alone container for testing use the command shown below:

    docker run -ti --name adcirc2cog_latest --volume /directory/path/to/storage:/data -d adcirc2cog /bin/bash

  After the container has been created, you can access it using the following command:

    docker exec -it adcirc2cog_latest bash

  To create tiffs and cogs you must first activate the conda enviroment using the following command:

    conda activate adcirc2cog

  Now you can run the command to create a tiff:

    python adcirc2geotiff.py --inputDIR /data/4221-2022080406-namforecast/input --outputDIR /data/4221-2022080406-namforecast/cogeo --inputFile maxele.63.nc

  and the command to create the cog file:

    python geotiff2cog.py --inputDIR /data/4221-2022080406-namforecast/cogeo --finalDIR /data/4221-2022080406-namforecast/final/cogeo --inputParam maxele63

  where 4221-2022080406-namforecast is any ADCIRC run. 
