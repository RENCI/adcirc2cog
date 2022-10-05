#!/usr/bin/env python

# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

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
    inputPathFiles = glob.glob(inputDir+'*.tif')

    # Check if inputPathFiles list has values
    if len(inputPathFiles) > 0:
        for inputPathFile in inputPathFiles:
            if os.path.exists(inputPathFile):
                # Log inputPathFile
                logger.info('The inputPathFile '+inputPathFile.strip()+' so create cog file.')

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

                # Define command to create cog
                cmds_list.append(['rio', 'cogeo', 'create',  inputPathFile, inputDir+outputFile, '--web-optimized'])
            else:
                logger.info('The inputPathFile '+inputPathFile+' does not exist.')
                sys.exit(1)
    else:
        logger.info('inputPathFiles list has not values')
        sys.exit(1)

    # Define number of CPU to use in pool.
    logger.info('Create pool.')
    pool = Pool(processes=4)
    logger.info('Pool created.')

    # Apply cmds_list to pool, and output to resutls
    logger.info('Create results array.')
    results = []
    for cmd in cmds_list:
        results.append(pool.apply_async(call_proc, (cmd,)))

    logger.info('Results array created.')

    # Close the pool and wait for each running task to complete
    logger.info('Close pool.')
    pool.close()
    pool.join()
    logger.info('Pool closed.')

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
def main(inputParam, inputDir, finalDir):
    logger.info('Create cog files in '+inputDir.strip()+' tiff file.')

    geotiff2cog(inputDir, finalDir)

    logger.info('Created cog files in '+inputDir.strip()+'.')

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

if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputPARAM", "--inputParam", help="Input parameter", action="store", dest="inputParam")
    parser.add_argument("--inputDIR", "--inputDir", help="Input directory path", action="store", dest="inputDir")
    parser.add_argument("--finalDIR", "--finalDir", help="Final directory path", action="store", dest="finalDir")

    args = parser.parse_args()

    # Remove old logger and start new logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'geotiff2cog.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")
    logger.info('Started log file geotiff2cog.log')

    # Get input variables from args
    inputParam = os.path.join(args.inputParam, '')
    inputDir = os.path.join(args.inputDir, '')
    finalDir = os.path.join(args.finalDir, '')
    inputDir = os.path.join(inputDir+inputParam, '')
    finalDir = os.path.join(finalDir+inputParam, '')
    logger.info('Got input variables including inputDir '+inputDir+'.')

    # Check if input file exists and if it does run geotiff2cog function
    if os.path.exists(inputDir):
        main(inputParam, inputDir, finalDir)
    else:
        logger.info(inputDir+inputParam+' does not exist')
        if inputParam.startswith("swan"):
            logger.info('The input file is a swan file so do a soft exit')
            sys.exit(0)
        else:
            logger.info('The input file is not a swan file so do a hard exit')
            sys.exit(1)

