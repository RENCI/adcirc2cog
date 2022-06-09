#!/usr/bin/env python
import sys, os, argparse, shutil, glob
from pathlib import Path
from loguru import logger
from subprocess import Popen, PIPE, STDOUT
from multiprocessing.pool import ThreadPool as Pool

# Function creates a process using command from cmd
def call_proc(cmd):
    # This runs in a separate thread
    p = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    stdout, stderr = p.communicate()
    return (stdout, stderr)

def geotiff2cog(inputDir, finalDir):
    # Create empty list for commands
    cmds_list = []

    # Get list of input tiff files
    for inputPathFile in glob.glob(inputDir+'*.tif'):

        # Check if input file exists and if it does run geotiff2cog function
        if os.path.exists(inputPathFile):
            # When error exit program
            logger.add(lambda _: sys.exit(1), level="ERROR")

            logger.info('Create cog file from '+inputPathFile.strip()+' tiff file.')

            # Define ouput cog file name
            inputFileList = inputPathFile.split('/')[-1].split('.')
            inputFileList.insert(-1,'cog')
            outputFile = ".".join(inputFileList)

            # Remove cog file if it already exits
            if os.path.exists(inputDir+outputFile):
                os.remove(inputDir+outputFile)
                logger.info('Removed old cog file '+inputDir+outputFile+'.')
                logger.info('Cogeo path '+inputDir+outputFile+'.')
            else:
                logger.info('Cogeo path '+inputDir+outputFile+'.')

            # Difine command to create cog
            cmds_list.append(['rio', 'cogeo', 'create',  inputPathFile, inputDir+outputFile, '--web-optimized'])

        else:
            logger.info(inputPathFile+' does not exist')
            sys.exit(1)

    # Define number of CPU to use in pool.
    pool = Pool(processes=4)

    # Apply cmds_list to pool, and output to resutls
    results = []
    for cmd in cmds_list:
        results.append(pool.apply_async(call_proc, (cmd,)))

    # Close the pool and wait for each running task to complete
    pool.close()
    pool.join()

    # Output results to log
    for result in results:
        stdout, stderr = result.get()

        if stderr:
            logger.info("stdout: {} stderr: {}".format(stdout, stderr))
            sys.exit(1)
        else:
            logger.info("stdout: {} stderr: {}".format(stdout, stderr))

    # Create final directory path
    if os.path.exists(finalDir):
        os.rmdir(finalDir)
        logger.info('Remove directory '+finalDir+ '.')
        os.makedirs(finalDir, exist_ok=True)
        logger.info('Made directory '+finalDir+ '.')
    else:
       os.makedirs(finalDir, exist_ok=True)
       logger.info('Directory '+finalDir+' does not exist so make it.')

    # Move cogs to final directory
    for finalPathFile in glob.glob(inputDir+'*.cog.tif'):
        try:
            shutil.move(finalPathFile, finalDir)
            logger.info('Moved cog file '+finalPathFile.split("/")[-1]+' to '+finalDir+' directory.')
        except OSError as err:
            logger.error('Failed to move cog file '+finalPathFile.split("/")[-1]+' to '+finalDir+' directory.')
            sys.exit(1)

@logger.catch
def main(args):
    # Get input variables from args
    inputParam = os.path.join(args.inputParam, '')
    inputDir = os.path.join(args.inputDir, '')
    finalDir = os.path.join(args.finalDir, '')
    inputDir = os.path.join(inputDir+inputParam, '')
    finalDir = os.path.join(finalDir+inputParam, '')
    
    # Remove old logger and start new logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'geotiff2cog.log', level='DEBUG')

    # Check if input file exists and if it does run geotiff2cog function 
    if os.path.exists(inputDir):
        # When error exit program
        logger.add(lambda _: sys.exit(1), level="ERROR")

        logger.info('Create cog files in '+inputDir.strip()+' tiff file.')

        geotiff2cog(inputDir, finalDir)

        logger.info('Created cog files in '+inputDir.strip()+'.')

        # If timeseries create meta files for mozaic
        if finalDir.split('/')[-2].lower().find('max') == -1:
            logger.info('Create meta file for timeseries mosaic COGs')
            f = open(finalDir+'indexer.properties', 'w')
            f.write('TimeAttribute=ingestion\nElevationAttribute=elevation\nSchema=*the_geom:Polygon,location:String,ingestion:java.util.Date,elevation:Integer\nPropertyCollectors=TimestampFileNameExtractorSPI[timeregex](ingestion)\n')
            f.close()

            f = open(finalDir+'timeregex.properties', 'w')
            f.write('regex=[0-9]{8}T[0-9]{6}\n')
            f.close()

            f = open(finalDir+'datastore.properties', 'w')
            f.write('SPI=org.geotools.data.postgis.PostgisNGDataStoreFactory\nhost=localhost\nport=5432\ndatabase=apsviz_cog_mosaic\nschema=public\nuser=apsviz_cog_mosaic\npasswd=cog_mosaic\nLoose\ bbox=true\nEstimated\ extends=false\nvalidate\ connections=true\nConnection\ timeout=10\npreparedStatements=true\n')
            f.close()

            # Zip finalDir into zip file, and then remove the finalDir
            logger.info('Zip finalDir '+finalDir)

            try:
                shutil.make_archive(finalDir[:-1], 'zip', root_dir="/".join(finalDir.split('/')[:-2]), base_dir=finalDir.split('/')[-2])
                logger.info('Ziped finalDir to FinalDir to zip file '+"/".join(finalDir.split('/')[:-2])+'/'+finalDir.split('/')[-2]+'.zip')
            except OSError as err:
                logger.error('Problem zipping file '+"/".join(finalDir.split('/')[:-2])+'/'+finalDir.split('/')[-2]+'.zip')
                sys.exit(1)

            try:
                shutil.rmtree(finalDir)
                logger.info('Removed finalDir '+finalDir)
            except OSError as err:
                logger.error('Problem removing finalDir '+finalDir)
                sys.exit(1)

        else:
            logger.info('Data is not timeseries so no need to create meta file')

            # Zip finalDir into zip file, and then remove the finalDir
            logger.info('Zip finalDir '+finalDir)

            try:
                shutil.make_archive(finalDir[:-1], 'zip', root_dir="/".join(finalDir.split('/')[:-2]), base_dir=finalDir.split('/')[-2])
                logger.info('Ziped finalDir to FinalDir to zip file '+"/".join(finalDir.split('/')[:-2])+'/'+finalDir.split('/')[-2]+'.zip')
            except OSError as err:
                logger.error('Problem zipping file '+"/".join(finalDir.split('/')[:-2])+'/'+finalDir.split('/')[-2]+'.zip')
                sys.exit(1)

            try:
                shutil.rmtree(finalDir)
                logger.info('Removed finalDir '+finalDir)
            except OSError as err:
                logger.error('Problem removing finalDir '+finalDir)
                sys.exit(1)

    else:
        logger.info(inputDir+' does not exist')
        sys.exit(1)

if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputPARAM", "--inputParam", help="Input parameter", action="store", dest="inputParam")
    parser.add_argument("--inputDIR", "--inputDir", help="Input directory path", action="store", dest="inputDir")
    parser.add_argument("--finalDIR", "--finalDir", help="Final directory path", action="store", dest="finalDir")

    args = parser.parse_args()
    main(args)

