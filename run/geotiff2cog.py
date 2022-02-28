#!/usr/bin/env python
import sys, os, argparse, shutil
from pathlib import Path
from loguru import logger
from subprocess import Popen, PIPE

def geotiff2cog(inputFile, inputDir, finalDir):
    # Create cog directory path
    if not os.path.exists(inputDir):
        #mode = 0o755
        #os.makedirs(inutDir, mode)
        os.makedirs(inputDir, exist_ok=True)
        logger.info('Made directory '+Path(inputDir).parts[-1]+ '.')
    else:
        logger.info('Directory '+Path(inputDir).parts[-1]+' already made.')

    # Define input tiff file name and path
    tiffFile = inputDir+inputFile

    # Define ouput cog file name
    outputFile = ".".join(inputFile.split('.')[0:3])+'.cog.tif'

    # Remove cog file if it already exits
    if os.path.exists(inputDir+outputFile):
        os.remove(inputDir+outputFile)
        logger.info('Removed old cog file '+inputDir+outputFile+'.')
        logger.info('Cogeo path '+inputDir+outputFile+'.')
    else:
        logger.info('Cogeo path '+inputDir+outputFile+'.')

    # Difine command to create cog
    cmds_list = [
      ['rio', 'cogeo', 'create',  tiffFile, inputDir+outputFile]
    ]

    # Define process list of commands
    procs_list = [Popen(cmd, stdout=PIPE, stderr=PIPE) for cmd in cmds_list]

    # Run process list of commands
    for proc in procs_list:
        proc.wait()

    logger.info('Created cog file '+outputFile+' from tiff file '+inputFile+'.')

    # Create final directory path
    if not os.path.exists(finalDir):
        # mode = 0o755
        # os.makedirs(finalDir, mode)
        os.makedirs(finalDir, exist_ok=True)
        logger.info('Made directory '+Path(finalDir).parts[-1]+ '.')
    else:
        logger.info('Directory '+Path(finalDir).parts[-1]+' already made.')

    # Move cog to final directory
    shutil.move(inputDir+outputFile, finalDir+outputFile)
    logger.info('Moved cog file to '+Path(finalDir).parts[-1]+' directory.')

@logger.catch
def main(args):
    # Get input variables from args
    inputFile = args.inputFile 
    inputDir = os.path.join(args.inputDir, '')
    finalDir = os.path.join(args.finalDir, '')

    # Remove old logger and start new logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'geotiff2cog.log', level='DEBUG')

    # Check if input file exists and if it does run geotiff2cog function 
    if os.path.exists(inputDir+inputFile):
        # When error exit program
        logger.add(lambda _: sys.exit(1), level="ERROR")

        logger.info('Create cog file from '+inputFile.strip()+' tiff file.')

        geotiff2cog(inputFile, inputDir, finalDir)

        logger.info('Created cog file.')
 
    else:
        logger.info(inputFile+' does not exist')
        sys.exit(0)

if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputFILE", "--inputFile", help="Input file name", action="store", dest="inputFile")
    parser.add_argument("--inputDIR", "--inputDir", help="Input directory path", action="store", dest="inputDir")
    parser.add_argument("--finalDIR", "--finalDir", help="Final directory path", action="store", dest="finalDir")

    args = parser.parse_args()
    main(args)

