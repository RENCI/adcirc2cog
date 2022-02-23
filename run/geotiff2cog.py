#!/usr/bin/env python
import sys, os, argparse, shutil, pdb
from loguru import logger
from subprocess import Popen, PIPE

def geotiff2cog(inputFile, inputDIR, finalDIR):
    # Create cog directory path
    if not os.path.exists(inputDIR):
        #mode = 0o755
        #os.makedirs(inutDIR, mode)
        os.makedirs(inputDIR, exist_ok=True)
        logger.info('Made directory '+inputDIR.split('/')[-1]+ '.')
    else:
        logger.info('Directory '+inputDIR.split('/')[-1]+' already made.')

    # Define input tiff file name and path
    tiffFile = inputDIR+'/'+inputFile

    # Define ouput cog file name
    outputFile = ".".join(inputFile.split('.')[0:3])+'.cog.tif'

    # Remove cog file if it already exits
    if os.path.exists(inputDIR+'/'+outputFile):
        os.remove(inputDIR+'/'+outputFile)
        logger.info('Removed old cog file '+inputDIR+'/'+outputFile+'.')
        logger.info('Cogeo path '+inputDIR+'/'+outputFile+'.')
    else:
        logger.info('Cogeo path '+inputDIR+'/'+outputFile+'.')

    # Difine command to create cog
    cmds_list = [
      ['rio', 'cogeo', 'create',  tiffFile, inputDIR+'/'+outputFile]
    ]

    # Define process list of commands
    procs_list = [Popen(cmd, stdout=PIPE, stderr=PIPE) for cmd in cmds_list]

    # Run process list of commands
    for proc in procs_list:
        proc.wait()

    logger.info('Created cog file '+outputFile+' from tiff file '+inputFile+'.')

    # Create final directory path
    if not os.path.exists(finalDIR):
        # mode = 0o755
        # os.makedirs(finalDIR, mode)
        os.makedirs(finalDIR, exist_ok=True)
        logger.info('Made directory '+finalDIR.split('/')[-1]+ '.')
    else:
        logger.info('Directory '+finalDIR.split('/')[-1]+' already made.')

    # Move cog to final directory
    shutil.move(inputDIR+'/'+outputFile, finalDIR+'/'+outputFile)
    logger.info('Moved cog file to '+finalDIR.split('/')[-1]+' directory.')

@logger.catch
def main(args):
    # Get input variables from args
    inputFile = args.inputFile 
    inputDIR = args.inputDIR
    finalDIR = args.finalDIR

    # Remove old logger and start new logger
    logger.remove()
    log_path = os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs'))
    logger.add(log_path+'/geotiff2cog.log', level='DEBUG')

    # Check if input file exists and if it does run geotiff2cog function 
    if os.path.exists(inputDIR+'/'+inputFile):
        # When error exit program
        logger.add(lambda _: sys.exit(1), level="ERROR")

        logger.info('Create cog file from '+inputFile.strip()+' tiff file.')

        geotiff2cog(inputFile, inputDIR, finalDIR)

        logger.info('Created cog file.')
 
    else:
        logger.info(inputFile+' does not exist')
        sys.exit(0)

if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputFile", action="store", dest="inputFile")
    parser.add_argument("--inputDIR", action="store", dest="inputDIR")
    parser.add_argument("--finalDIR", action="store", dest="finalDIR")

    args = parser.parse_args()
    main(args)

