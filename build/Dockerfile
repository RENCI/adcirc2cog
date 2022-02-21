##############
# Docker file for the creation of the adcirc geotiff and cog files.
#
# to create image: docker build -t adcirc2cog:latest .
# to push image:
#       docker tag adcirc2cog:latest renciorg/adcirc2cog:latest
#       docker push renciorg/adcirc2cog:latest
##############
# need at least 103 GB to process to tiffs. This includes 102 GB for the tiffs, and 1 GB for the netCDF files.
# need an additional 2 GB for the cog files, for a total of 105 GB. Round off to 110 GB for good luck.
# need 28+ GB for the final color tiffs, and 70+ GB for the raw tiffs. The raw tiffs will be deleted after 
# they have been used to produce the color tiffs. We plan to save the color tiffs as well as the cog.
# to process one maxele.63.nc file to tiff, I will need 52+ MB for the netCDF file, 14+ GB for the raw tiff
# file and 7 GB for the color tiff file, for a total of 22 GB.
# set the base image
##############
FROM continuumio/miniconda

# author
MAINTAINER Jim McManus

# extra metadata
LABEL version="1.0"
LABEL description="adcirc2cog image with Dockerfile."

# update sources list, and install basic apps, one per line for better caching
RUN apt-get clean && apt-get --allow-releaseinfo-change update &&\
    apt-get install -qy vim \
    libgl1 \
    libgl1-mesa-dev \
    aria2

# update conda and set the download channel
RUN conda update conda && \
    conda config --add channels conda-forge

WORKDIR /app

# Create the environment, initialize conda in bash config files, and install repos:
COPY environment.yml .
RUN conda env create -f environment.yml

# make d directory for the repos and go there
RUN mkdir /repos
WORKDIR /repos

# get the repo
RUN git clone -b lowres https://github.com/renci/adcirc2cog.git

# change to the run directory                        
WORKDIR /repos/adcirc2cog/run

# set the python path
ENV PYTHONPATH=/repos/adcirc2cog
