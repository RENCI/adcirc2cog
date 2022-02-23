#!/usr/bin/env python
import os, sys, argparse, shutil, json, warnings
from loguru import logger
from functools import wraps
import netCDF4 as nc

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
    sys.path.append('/opt/conda/envs/adcirc2geotiff/share/qgis')
    sys.path.append('/opt/conda/envs/adcirc2geotiff/share/qgis/python/plugins')
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

def makeDIRS(outputDIR):
    # Create tiff directory path
    if not os.path.exists(outputDIR):
        # mode = 0o755
        # os.makedirs(outputDIR, mode)
        os.makedirs(outputDIR, exist_ok=True)
        logger.info('Made directory '+outputDIR.split('/')[-1]+ '.')
    else:
        logger.info('Directory '+outputDIR.split('/')[-1]+' already made.')

def getParameters(dirPath, inputFile, outputDIR):
    tiffile = inputFile.split('.')[0]+'.raw.'+inputFile.split('.')[1]+'.tif'
    parms = '{"INPUT_EXTENT" : "-97.85833,-60.040029999999994,7.909559999999999,45.83612", "INPUT_GROUP" : 1, "INPUT_LAYER" : "'+dirPath+'input/'+inputFile+'", "INPUT_TIMESTEP" : 0,  "OUTPUT_RASTER" : "'+outputDIR+'/'+tiffile+'", "MAP_UNITS_PER_PIXEL" : 0.005}'
    return(json.loads(parms))

# Convert mesh layer as raster and save as a GeoTiff
@ignore_warnings
def exportRaster(parameters):
    # Open layer from inputFile 
    inputFile = 'Ugrid:'+'"'+parameters['INPUT_LAYER']+'"'
    meshfile = inputFile.strip().split('/')[-1]
    meshlayer = meshfile.split('.')[0]
    layer = QgsMeshLayer(inputFile, meshlayer, 'mdal')

    ds = nc.Dataset(parameters['INPUT_LAYER'])
    for dim in ds.dimensions.values():
        if dim.size == 0:
            logger.info('The netCDF file '+meshfile.split('"')[0]+' has an invalid dimension value of 0, so the program will exit')
            sys.exit(0)

    # Check if layer is valid
    if layer.isValid() is True:
        # Get parameters for processing
        dataset  = parameters['INPUT_GROUP'] 
        timestep = parameters['INPUT_TIMESTEP']
        mupp = parameters['MAP_UNITS_PER_PIXEL'] 
        extent = layer.extent()
        output_layer = parameters['OUTPUT_RASTER']
        width = extent.width()/mupp 
        height = extent.height()/mupp 
        crs = layer.crs() 
        crs.createFromSrid(4326)
        transform_context = QgsProject.instance().transformContext()
        output_format = QgsRasterFileWriter.driverForExtension(os.path.splitext(output_layer)[1])

        # Open output file for writing
        rfw = QgsRasterFileWriter(output_layer)
        rfw.setOutputProviderKey('gdal') 
        rfw.setOutputFormat(output_format) 

        # Create one band raster
        rdp = rfw.createOneBandRaster( Qgis.Float64, width, height, extent, crs)

        # Get dataset index
        dataset_index = QgsMeshDatasetIndex(dataset, timestep)

        # Regred mesh layer to raster
        block = QgsMeshUtils.exportRasterBlock( layer, dataset_index, crs,
                transform_context, mupp, extent) 

        # Write raster to GeoTiff file
        rdp.writeBlock(block, 1)
        rdp.setNoDataValue(1, block.noDataValue())
        rdp.setEditable(False)

        logger.info('Regridded mesh data in '+meshfile.split('"')[0]+' to float64 grid, and saved to tiff ('+output_layer.split('/')[-1]+') file.')

        return(output_layer)

    if layer.isValid() is False: 
        raise Exception('Invalid mesh')

# Move raw tiff file to final/tiff
def moveRaw(inputFile, outputDIR, finalDIR):
    # Create final/tiff directory path
    if not os.path.exists(finalDIR):
        mode = 0o755
        # os.makedirs(finalDIR, mode)
        os.makedirs(finalDIR, exist_ok=True)
        logger.info('Made directory '+finalDIR.split('/')[-1]+ '.')
    else:
        logger.info('Directory '+finalDIR.split('/')[-1]+' already made.')

    tiffraw = inputFile.split('.')[0]+'.raw.'+inputFile.split('.')[1]+'.tif'
    # Check if raw tiff exists, and move it.
    if os.path.exists(outputDIR+'/'+tiffraw):
        shutil.move(outputDIR+'/'+tiffraw, finalDIR+'/'+tiffraw)
        os.remove(outputDIR+'/'+tiffraw+'.aux.xml')
        logger.info('Moved raw tiff file '+tiffraw+ 'to final/tiff directory.')
    else:
        logger.info('Raw tiff file '+rawtiff+' does not exist.')

@logger.catch
def main(args):
    inputFile = args.inputFile

    outputDIR = args.outputDIR
    finalDIR = args.finalDIR

    dirPath = "/".join(outputDIR.split('/')[0:-1])+'/'

    logger.remove()
    log_path = os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs'))
    logger.add(log_path+'/adcirc2geotiff-logs.log', level='DEBUG')

    if os.path.exists(dirPath+'input/'+inputFile):
        # When error exit program
        logger.add(lambda _: sys.exit(1), level="ERROR")

        makeDIRS(outputDIR.strip())

        os.environ['QT_QPA_PLATFORM']='offscreen'
        xdg_runtime_dir = '/run/user/adcirc2geotiff'
        os.makedirs(xdg_runtime_dir, exist_ok=True)
        os.environ['XDG_RUNTIME_DIR']=xdg_runtime_dir
        logger.info('Set QGIS enviroment.')

        app = initialize_qgis_application() 
        app.initQgis()
        app, processing = initialize_processing(app)
        logger.info('Initialzed QGIS.')

        parameters = getParameters(dirPath, inputFile.strip(), outputDIR.strip())
        logger.info('Got mesh regrid paramters for '+inputFile.strip())

        filename = exportRaster(parameters)

        app.exitQgis()
        logger.info('Quit QGIS')

        moveRaw(inputFile, outputDIR, finalDIR)
        logger.info('Moved float64 tiff file')

    else:
         logger.info(inputFile+' does not exist')
         sys.exit(0)

if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputFile", action="store", dest="inputFile")
    parser.add_argument("--outputDIR", action="store", dest="outputDIR")
    parser.add_argument("--finalDIR", action="store", dest="finalDIR")

    args = parser.parse_args()
    main(args)

