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

# add user nru
RUN useradd -m -d /home/nru -u 1000 nru
RUN chmod 777 /home/nru
USER nru

# make app directory in nru
RUN mkdir /home/nru/app
WORKDIR /home/nru/app
RUN chmod 777 -R .

# Create the environment, initialize conda in bash config files, and install repos:
COPY environment.yml .
RUN conda env create -f environment.yml

# make d directory for the repos and go there
RUN mkdir /home/nru/repos
WORKDIR /home/nru/repos
RUN chmod 777 -R .

# get the repo
RUN git clone https://github.com/RENCI/adcirc2cog.git

# change to the run directory                        
WORKDIR /home/nru/repos/adcirc2cog/run

# set the python path
ENV PYTHONPATH=/home/nru/repos/adcirc2cog

# tell conda what the shell is
RUN conda init bash
