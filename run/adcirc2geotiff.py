#!/usr/bin/env python

# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

# Import Python modules
import os, sys, argparse, shutil, json, warnings, pdb
import netCDF4 as nc
import numpy as np
from datetime import datetime
from datetime import timedelta
from loguru import logger
from functools import wraps

# Import QGIS modules
from PyQt5.QtGui import QColor
from qgis.core import (
    Qgis,
    QgsApplication,
    QgsMeshLayer,
    QgsMeshDatasetIndex,
    QgsMeshUtils,
    QgsProject,
    QgsRasterLayer,
    QgsRasterFileWriter,
    QgsRectangle,
    QgsErrorMessage
)

# Ignore warning function
def ignore_warnings(f):
    @wraps(f)
    def inner(*args, **kwargs):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("ignore")
            response = f(*args, **kwargs)
        return response
    return inner

# Initialize application
def initialize_qgis_application():
    sys.path.append('/home/nru/.conda/envs/adcirc2cog/share/qgis')
    sys.path.append('/home/nru/.conda/envs/adcirc2cog/share/qgis/python/plugins')
    app = QgsApplication([], False)
    return (app)

# Add the path to processing so we can import it next
@ignore_warnings # Ignored because we want the output of this script to be a single value, and "import processing" is noisy
def initialize_processing(app):
    # import processing module
    import processing
    from processing.core.Processing import Processing

    # Initialize Processing
    Processing.initialize()
    return (app, processing)

# Make output directory if it does not exist
def makeDirs(outputDir):
    # Create cogeo directory path
    if not os.path.exists(outputDir):
        mode = 0o777
        os.makedirs(outputDir, mode, exist_ok=True)
        logger.info('Made directory '+outputDir+ '.')
    else:
        logger.info('Directory '+outputDir+' already made.')

# Define parameters used in creating tiff
def getParameters(inputDir, inputFile, outputDir, outputFile, inputExtent, timeStep, mapUnitPP):
    parms = '{"INPUT_EXTENT" : "'+inputExtent+'", "INPUT_GROUP" : 1, "INPUT_LAYER" : "'+inputDir+inputFile+'", "INPUT_TIMESTEP" : '+str(timeStep)+',  "OUTPUT_RASTER" : "'+outputDir+outputFile+'", "MAP_UNITS_PER_PIXEL" : '+str(mapUnitPP)+'}'
    return(json.loads(parms))

@ignore_warnings
class mesh2tiff:
    def __init__(self, inputDir, outputDir, inputFile, tmpDir):
        # Open layer from INPUT_LAYER
        logger.info('Open layer from input '+inputDir+inputFile+' file.')
        inputMeshFile = 'Ugrid:'+'"'+inputDir+inputFile+'"'
        meshFile = inputFile.strip().split('/')[-1]
        meshLayer = meshFile.split('.')[0]
        self.layer = QgsMeshLayer(inputMeshFile, meshLayer, 'mdal')

        # Define self.tmpDir
        self.tmpDir = tmpDir

        # Open INPUT_LAYER with netCDF4, and check its dimensions. If dimensions are incorrect exit program
        logger.info('Check INPUT_LAYER '+inputDir+inputFile+' dimensions')
        ds = nc.Dataset(inputDir+inputFile)
        times = list(ds.variables['time'][:].data)
        year, month, day = ds.variables['time'].base_date.split(' ')[0].split('-')
        hour, minute, second = ds.variables['time'].base_date.split(' ')[1].split(':')
        base_date = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second)) 
        for dim in ds.dimensions.values():
            if dim.size == 0:
                logger.info('The netCDF file '+meshFile.split('"')[0]+' has an invalid dimension value of 0, so the program will exit')
                sys.exit(1)
        ds.close()

        # Define timestep
        timeStep = 0

        # Define map units per pixel
        mapUnitsPP = [0.01, 0.005, 0.001, 0.001, 0.005, 0.005, 0.005, 0.005, 0.005]

        # Define fileDateTime
        fileDateTime = datetime.fromisoformat(str(base_date + timedelta(seconds=times[0]))).strftime("%Y%m%dT%H%M%S")
 
        # Define input extent parameters to create tiff from ADCIRC mesh file
        inputExtents = ['-97.85833,-77.5833,31.0,45.83612',
                        '-77.85833,-60.040029999999994,36.0,45.83612',
                        '-97.85833,-81.0,23.0,31.273088',
                        '-81.937856,-74.0,23.0,36.555922',
                        '-74.467152,-60.040029999999994,23.0,36.83612',
                        '-97.85833,-77.0,15.0,23.213514',
                        '-77.85833,-60.040029999999994,15.0,23.213514',
                        '-97.85833,-77.0,7.909559999999999,15.83612',
                        '-77.85833,-60.040029999999994,7.909559999999999,15.83612']

        # Define input_list and outputFile index
        inputs_list = []
        i = 0

        # Add variables to input_list
        logger.info('Create inputs_list, which has multiple extents, and one timeStep')
        for inputExtent in inputExtents:
            inputFileList = inputFile.split('.')
            inputFileList.insert(1,'raw')
            inputFileList.insert(1,'subset'+str(i))
            inputFileList[-1] = 'tif'
            outputFile = ".".join(inputFileList)
            mapUnitPP = mapUnitsPP[i]

            inputs_list.append([inputDir, inputFile, outputDir, outputFile, inputExtent, timeStep, mapUnitPP])

            i = i + 1


        # Run exportRaster using multiprocessinng for loop, and imput_list
        logger.info('Run exportRaster in for loop, with inputs_list')
        for inputList in inputs_list:
           self.exportRaster(inputList) 

        self.layer = None

    # Convert mesh layer as raster and save as a GeoTiff
    def exportRaster(self, inputList):
        # Get input from inputList
        inputDir = inputList[0]
        inputFile = inputList[1]
        outputDir = inputList[2]
        outputFile = inputList[3]
        inputExtent = inputList[4]
        timeStep = inputList[5]
        mapUnitPP = inputList[6]

        # Get parameters
        parameters = getParameters(inputDir, inputFile, outputDir, outputFile, inputExtent, timeStep, mapUnitPP)

        # Check if layer is valid
        if self.layer.isValid() is True:
            # Get parameters for processing
            logger.info('Get parameters for '+parameters['INPUT_LAYER']+'.')
            dataset  = parameters['INPUT_GROUP'] 
            timestep = parameters['INPUT_TIMESTEP']
            mupp = parameters['MAP_UNITS_PER_PIXEL'] 
            input_extent = parameters['INPUT_EXTENT'].split(',')
            extent = QgsRectangle(float(input_extent[0]),float(input_extent[2]),float(input_extent[1]),float(input_extent[3]))
            output_layer = parameters['OUTPUT_RASTER']
            width = extent.width()/mupp 
            height = extent.height()/mupp 
            crs = self.layer.crs() 
            crs.createFromSrid(4326)

            # Transform instance
            logger.info('Transform instance of '+parameters['INPUT_LAYER']+'.')
            transform_context = QgsProject.instance().transformContext()
            output_format = QgsRasterFileWriter.driverForExtension(os.path.splitext(output_layer)[1])

            # Open output file for writing
            logger.info('Open output file '+output_layer+'.')
            rfw = QgsRasterFileWriter(output_layer)
            rfw.setOutputProviderKey('gdal') 
            rfw.setOutputFormat(output_format) 

            # Create one band raster
            logger.info('Create one band raster')
            rdp = rfw.createOneBandRaster( Qgis.Float64, width, height, extent, crs)

            # Get dataset index
            logger.info('Get data set index')
            dataset_index = QgsMeshDatasetIndex(dataset, timestep)

            # Regred mesh layer to raster
            logger.info('Regrid mesh layer '+inputDir+inputFile+'.' )
            os.chdir(self.tmpDir)
            block = QgsMeshUtils.exportRasterBlock( self.layer, dataset_index, crs,
                    transform_context, mupp, extent) 
            os.chdir('/home/nru/adcirc2cog/run')

            # Write raster to GeoTiff file
            logger.info('Write raster Geotiff ('+output_layer+') file.')
            rdp.writeBlock(block, 1)
            rdp.setNoDataValue(1, block.noDataValue())
            rdp.setEditable(False)

            block = None

            logger.info('Regridded mesh data in '+inputDir+inputFile+' to float64 grid, and saved to tiff ('+output_layer+') file.')

        if self.layer.isValid() is False: 
            raise Exception('Invalid mesh ('+inputDir+inputFile+') file.')

@logger.catch
def main(inputDir, outputDir, inputFile):
    # Define tmp directory
    tmpDir = "/".join(inputDir.split("/")[:-2])+"/"+inputFile.split('.')[0]+"_qgis_tmp/"
    logger.info('Create tmpDir: '+tmpDir+' for QGIS')

    # Make output directory
    makeDirs(outputDir.strip())

    # Set QGIS environment 
    os.environ['QT_QPA_PLATFORM']='offscreen'
    xdg_runtime_dir = '/home/nru/adcirc2geotiff'
    os.makedirs(xdg_runtime_dir, exist_ok=True)
    os.environ['XDG_RUNTIME_DIR']=xdg_runtime_dir
    os.makedirs(tmpDir, exist_ok=True)
    os.environ['TMPDIR'] = tmpDir
    logger.info('Set QGIS enviroment.')

    # Check if tmpDir exists
    if not os.path.exists(tmpDir):
        logger.error('The tmpDir: '+tmpDir+' does not exist')
        sys.exit(1)
    elif os.path.exists(tmpDir):
        logger.info('The tmpDir: '+tmpDir+' does exist')
    else:
        logger.error('Checked for tmpDir: '+tmpDir+', and else statement happened')
        sys.exit(1)

    # Initialize QGIS
    app = initialize_qgis_application() 
    app.initQgis()
    app, processing = initialize_processing(app)
    logger.info('Initialzed QGIS.')

    # Run mesh2tiff and producer tiff files
    mesh2tiff(inputDir, outputDir, inputFile, tmpDir)

    # Quit QGIS
    app.exitQgis()
    logger.info('Quit QGIS')

if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputDIR", "--inputDir", help="Input directory path", action="store", dest="inputDir", required=True)
    parser.add_argument("--outputDIR", "--outputDir", help="Output directory path", action="store", dest="outputDir", required=True)
    parser.add_argument("--inputFILE", "--inputFile", help="Input file name", action="store", dest="inputFile", required=True)
    args = parser.parse_args()

    # Remove old logger and start new one
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'adcirc2geotiff_vcog.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")
    logger.info('Started log file adcirc2geotiff_vcog.log')

    # get input variables from args
    inputDir = os.path.join(args.inputDir, '')
    outputDir = os.path.join(args.outputDir, '')
    inputFile = args.inputFile
    logger.info('Got input variables including inputDir '+inputDir+'.')

    logger.info('Modify outputDir: '+outputDir)
    outputDir = os.path.join(outputDir+"".join(inputFile[:-3].split('.')), '')
    logger.info('Modified outputDir: '+outputDir)

    if os.path.exists(inputDir+inputFile):
        main(inputDir, outputDir, inputFile)
    else:
         logger.info(inputDir+inputFile+' does not exist')
         if inputFile.startswith("swan"):
             logger.info('The input file is a swan file : '+inputDir+inputFile+' so do a soft exit') 
             sys.exit(0)
         else:
             logger.info('The input file is not a swan file : '+inputDir+inputFile+' so do a hard exit')
             sys.exit(1)

