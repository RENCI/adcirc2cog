#!/usr/bin/env python
# Import Python modules
import os, sys, argparse, shutil, json, warnings
import netCDF4 as nc
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

# Make output directory if it does not exist
def makeDirs(outputDir):
    # Create cogeo directory path
    if not os.path.exists(outputDir):
        # mode = 0o755
        # os.makedirs(outputDir, mode)
        os.makedirs(outputDir, exist_ok=True)
        logger.info('Made directory '+outputDir+ '.')
    else:
        logger.info('Directory '+outputDir+' already made.')

# Define parameters used in creating tiff
def getParameters(inputDir, inputFile, outputDir):
    tiffile = inputFile.split('.')[0]+'.raw.'+inputFile.split('.')[1]+'.tif'
    parms = '{"INPUT_EXTENT" : "-97.85833,-60.040029999999994,7.909559999999999,45.83612", "INPUT_GROUP" : 1, "INPUT_LAYER" : "'+inputDir+inputFile+'", "INPUT_TIMESTEP" : 0,  "OUTPUT_RASTER" : "'+outputDir+tiffile+'", "MAP_UNITS_PER_PIXEL" : 0.005}'
    return(json.loads(parms))

# Convert mesh layer as raster and save as a GeoTiff
@ignore_warnings
def exportRaster(parameters):
    # Open layer from INPUT_LAYER
    logger.info('Open layer from INPUT_LAYER')
    inputFile = 'Ugrid:'+'"'+parameters['INPUT_LAYER']+'"'
    meshfile = inputFile.strip().split('/')[-1]
    meshlayer = meshfile.split('.')[0]
    layer = QgsMeshLayer(inputFile, meshlayer, 'mdal')

    # Open INPUT_LAYER with netCDF4, and check its dimensions. If dimensions are incorrect exit program
    logger.info('Check INPUT_LAYER dimensions')
    ds = nc.Dataset(parameters['INPUT_LAYER'])
    for dim in ds.dimensions.values():
        if dim.size == 0:
            logger.info('The netCDF file '+meshfile.split('"')[0]+' has an invalid dimension value of 0, so the program will exit')
            sys.exit(0)

    # Check if layer is valid
    if layer.isValid() is True:
        # Get parameters for processing
        logger.info('Get parameters')
        dataset  = parameters['INPUT_GROUP'] 
        timestep = parameters['INPUT_TIMESTEP']
        mupp = parameters['MAP_UNITS_PER_PIXEL'] 
        extent = layer.extent()
        output_layer = parameters['OUTPUT_RASTER']
        width = extent.width()/mupp 
        height = extent.height()/mupp 
        crs = layer.crs() 
        crs.createFromSrid(4326)

        # Transform instance
        logger.info('Transform instance')
        transform_context = QgsProject.instance().transformContext()
        output_format = QgsRasterFileWriter.driverForExtension(os.path.splitext(output_layer)[1])

        # Open output file for writing
        logger.info('Open output file')
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
        logger.info('Regrid mesh layer')
        block = QgsMeshUtils.exportRasterBlock( layer, dataset_index, crs,
                transform_context, mupp, extent) 

        # Write raster to GeoTiff file
        logger.info('Write raster Geotiff file')
        rdp.writeBlock(block, 1)
        rdp.setNoDataValue(1, block.noDataValue())
        rdp.setEditable(False)

        logger.info('Regridded mesh data in '+meshfile.split('"')[0]+' to float64 grid, and saved to tiff ('+output_layer.split('/')[-1]+') file.')

        return(output_layer)

    if layer.isValid() is False: 
        raise Exception('Invalid mesh')

@logger.catch
def main(args):
    # get input variables from args
    inputFile = args.inputFile
    inputDir = os.path.join(args.inputDir, '')
    outputDir = os.path.join(args.outputDir, '')

    # Remove old logger and start new one
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'adcirc2geotiff_vcog.log', level='DEBUG')

    # Check to see if input directory exits and if it does create tiff
    if os.path.exists(inputDir+inputFile):
        # When error exit program
        logger.add(lambda _: sys.exit(1), level="ERROR")

        # Make output directory
        makeDirs(outputDir.strip())

        # Set QGIS environment 
        os.environ['QT_QPA_PLATFORM']='offscreen'
        xdg_runtime_dir = '/home/nru/adcirc2geotiff'
        os.makedirs(xdg_runtime_dir, exist_ok=True)
        os.environ['XDG_RUNTIME_DIR']=xdg_runtime_dir
        logger.info('Set QGIS enviroment.')

        # Initialize QGIS
        app = initialize_qgis_application() 
        app.initQgis()
        app, processing = initialize_processing(app)
        logger.info('Initialzed QGIS.')

        # get parameters to create tiff from ADCIRC mesh file
        parameters = getParameters(inputDir, inputFile.strip(), outputDir.strip())
        logger.info('Got mesh regrid paramters for '+inputFile.strip())

        # Create raw tiff file 
        filename = exportRaster(parameters)

        # Quit QGIS
        app.exitQgis()
        logger.info('Quit QGIS')

    else:
         logger.info(inputFile+' does not exist')
         sys.exit(0)

if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputFILE", "--inputFile", help="Input file name", action="store", dest="inputFile", required=True)
    parser.add_argument("--inputDIR", "--inputDir", help="Input directory path", action="store", dest="inputDir", required=True)
    parser.add_argument("--outputDIR", "--outputDir", help="Output directory path", action="store", dest="outputDir", required=True)

    args = parser.parse_args()
    main(args)

