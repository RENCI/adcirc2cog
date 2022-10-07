# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

##############
# Docker file for the creation of the adcirc geotiff and cog files.
#
# to create image: docker build -t adcirc2cog:latest .
# to push image:
#       docker tag adcirc2cog:latest renciorg/adcirc2cog:latest
#       docker push renciorg/adcirc2cog:latest
##############
FROM continuumio/miniconda3 as build

# author
MAINTAINER Jim McManus

# extra metadata
LABEL version="v0.0.9"
LABEL description="adcirc2cog image with Dockerfile."

# update conda
RUN conda update conda

# Create the virtual environment
COPY build/environment.yml .
RUN conda env create -f environment.yml

# install conda pack to compress this stage
RUN conda install -c conda-forge conda-pack

# conpress the virtual environment
RUN conda-pack -n adcirc2cog -o /tmp/env.tar && \
  mkdir /venv && cd /venv && tar xf /tmp/env.tar && \
  rm /tmp/env.tar

# fix up the paths
RUN /venv/bin/conda-unpack

##############
# stage 2: create a python implementation using the stage 1 virtual environment
##############
FROM python:3.9-slim

RUN apt-get update

# install wget and bc
RUN apt-get install -y wget bc vim libgl1 libgl1-mesa-dev aria2

# clear out the apt cache
RUN apt-get clean

# add user nru and switch to it
RUN useradd --create-home -u 1000 nru
RUN mkdir -p /data
RUN chown nru:nru /data
USER nru

# Create a directory for the log
RUN mkdir -p /home/nru/adcirc2cog/logs

# move the the code location
WORKDIR /home/nru/adcirc2cog

# Copy /venv from the previous stage:
COPY --from=build /venv /venv

# make the virtual environment active
ENV VIRTUAL_ENV /venv
ENV PATH /venv/bin:$PATH

# Copy in the rest of the code
RUN mkdir -p /home/nru/adcirc2cog/run
COPY run/adcirc2geotiff.py run/adcirc2geotiff.py
COPY run/geotiff2cog.py run/geotiff2cog.py

# set the python path
ENV PYTHONPATH="/venv/share/qgis/python:/venv/share/qgis/python/plugins:/venv/lib/:/home/nru/adcirc2cog/run"

# set the location of the output directory
ENV RUNTIMEDIR=/data
ENV PKLDIR=/data/pkldir

# set the log dir. use this for debugging if desired
ENV LOG_PATH=/data/logs

# example command line
# python adcirc2geotiff.py --inputDIR /data/4271-33-nhcOfcl/input --outputDIR /data/4271-33-nhcOfcl/cogeo --inputFile maxele.63.nc
# python geotiff2cog.py --inputDIR /data/4271-33-nhcOfcl/cogeo --finalDIR /data/4271-33-nhcOfcl/final/cogeo --inputParam maxele63

