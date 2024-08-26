'''
geotiff2cog converts and geoTiff image to a cloud optimized geo tiff image
'''
#!/usr/bin/env python

# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

import sys
import os
import argparse
import shutil
import glob
from subprocess import Popen, PIPE, STDOUT
from multiprocessing.pool import ThreadPool as Pool

from loguru import logger

# Function creates a process using command from cmd
def call_proc(cmd):
    ''' 
    This runs in a separate thread
    '''
    with Popen(cmd, stdout=PIPE, stderr=STDOUT) as proc:
        stdout, stderr = proc.communicate()
        return (stdout, stderr)

def geotiff2cog(**kwargs):
    ''' 
    Create empty list for commands
    '''
    cmds_list = []

    # Get list of input tiff files
    inputPathFiles = glob.glob(kwargs['inputParamDir']+'*.tif')

    # Check if inputPathFiles list has values
    if len(inputPathFiles) > 0:
        for inputPathFile in inputPathFiles:
            # Log inputPathFile
            logger.info('The inputPathFile '+inputPathFile.strip()+' so create cog file.')

            # Define ouput cog file name
            inputFileList = inputPathFile.split('/')[-1].split('.')
            inputFileList.insert(-1,'cog')
            outputFile = ".".join(inputFileList)

            # Remove cog file if it already exits
            if os.path.exists(kwargs['inputParamDir']+outputFile):
                os.remove(kwargs['inputParamDir']+outputFile)
                logger.info('Removed old cog file '+str(kwargs['inputParamDir'])+outputFile+'.')
                logger.info('Cogeo path '+kwargs['inputParamDir']+outputFile+'.')
            else:
                logger.info('Cogeo path '+kwargs['inputParamDir']+outputFile+'.')

            # Define command to create cog
            cmds_list.append(['rio', 'cogeo', 'create', inputPathFile,
                              kwargs['inputParamDir']+outputFile, '--web-optimized'])
    else:
        logger.info('inputPathFiles list has not values')
        sys.exit(1)

    # Define number of CPU to use in pool.
    logger.info('Create pool.')
    pool = Pool(processes=4)
    logger.info('Pool created.')

    # Apply cmds_list to pool, and output to resutls
    logger.info('Create results array.')
    for cmd in cmds_list:
        logger.info(" ".join(cmd))
        result = pool.apply_async(call_proc, (cmd,))
        stdout, stderr = result.get()

        if stderr:
            logger.info(f"stdout: {stdout} stderr: {stderr}")
            sys.exit(1)
        else:
            logger.info(f"stdout: {stdout} stderr: {stderr}")

    logger.info('Results array created.')

    # Close the pool and wait for each running task to complete
    logger.info('Close pool.')
    pool.close()
    pool.join()
    logger.info('Pool closed.')

    # Create final directory path
    if os.path.exists(kwargs['finalParamDir']):
        os.rmdir(kwargs['finalParamDir'])
        logger.info('Remove directory '+kwargs['finalParamDir']+'.')
        os.makedirs(kwargs['finalParamDir'], exist_ok=True)
        logger.info('Made directory '+kwargs['finalParamDir']+'.')
    else:
        os.makedirs(kwargs['finalParamDir'], exist_ok=True)
        logger.info('Directory '+kwargs['finalParamDir']+' does not exist so make it.')

    # Move cogs to final directory
    for finalPathFile in glob.glob(kwargs['inputParamDir']+'*.cog.tif'):
        try:
            shutil.move(finalPathFile, kwargs['finalParamDir'])
            logger.info('Moved cog file '+finalPathFile.split("/")[-1]+
            ' to '+kwargs['finalParamDir']+' directory.')
        except OSError as err:
            logger.exception(err)

@logger.catch
def main(**kwargs):
    '''
    This is the main function
    '''
    logger.info('Create cog files in '+kwargs['inputDirPath']+' tiff file.')

    geotiff2cog(inputParamDir = kwargs['inputDirPath'], finalParamDir = kwargs['finalDirPath'])

    logger.info('Created cog files in '+kwargs['inputDirPath']+'.')

    # Zip finalDir into zip file, and then remove the finalDir
    logger.info('Zip finalDir '+kwargs['finalDirPath'],)
    try:
        shutil.make_archive(kwargs['finalDirPath'][:-1], 'zip',
                            root_dir="/".join(kwargs['finalDirPath'].split('/')[:-2]),
                            base_dir=kwargs['finalDirPath'].split('/')[-2])
        logger.info('Ziped finalDir to FinalDir to zip file '+
                    "/".join(kwargs['finalDirPath'].split('/')[:-2])+
                    '/'+kwargs['finalDirPath'].split('/')[-2]+'.zip')
    except OSError as err:
        logger.exception(err)

    try:
        shutil.rmtree(kwargs['finalDirPath'])
        logger.info('Removed finalDir '+kwargs['finalDirPath'])
    except OSError as err:
        logger.exception(err)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputPARAM", "--inputParam", help="Input parameter",
                        action="store", dest="inputParam")
    parser.add_argument("--inputDIR", "--inputDir", help="Input directory path",
                        action="store", dest="inputDir")
    parser.add_argument("--finalDIR", "--finalDir", help="Final directory path",
                        action="store", dest="finalDir")

    args = parser.parse_args()

    # Remove old logger and start new logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH',
                                      os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'geotiff2cog.log', level='DEBUG', rotation="1 MB")
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
        main(inputDirPath = inputDir, finalDirPath = finalDir)
    else:
        logger.info(inputDir+inputParam+' does not exist')
        if inputParam.startswith("swan"):
            logger.info('The input file is a swan file so do a soft exit')
            sys.exit(0)
        else:
            logger.info('The input file is not a swan file so do a hard exit')
            sys.exit(1)
