import argparse
import logging
import time
import os.path

from cax.config import mongo_password
from cax.config import set_json
from cax.tasks import checksum, clear, data_mover, process


def single():
    raise RuntimeError("cax-single has been removed: use cax --once instead")


def main():
    parser = argparse.ArgumentParser(description="Copying All kinds of XENON1T data.")
    parser.add_argument('--once', action='store_true',
                        help="Run all tasks just one, then exits")
    parser.add_argument('--config', action='store', dest='val',
                        help="Load a specific *json file into cax")

    
    args = parser.parse_args()
    run_once = args.once

    #Define a specific cax.json configuration file for cax:
    caxjson_config = args.val
    if caxjson_config == None or os.path.isfile( caxjson_config ) == False :
      caxjson_config = 'cax.json'
      print('-----------------------------------------------------')
      print('There is no specific *json specified for running cax.')
      print('Use the standard one: cax.json')
      print('-----------------------------------------------------')
    else:
      print('-----------------------------------------------------')
      print('Json configuration file: ', caxjson_config )
      print('-----------------------------------------------------')
    set_json( caxjson_config )

    #exit()

    # Check passwords and API keysspecified
    mongo_password()

    # Setup logging
    logging.basicConfig(filename='cax.log',
                        level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] %(message)s')
    logging.info('Daemon is starting')

    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()

    console.setLevel(logging.INFO)

    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)

    tasks = [
             process.ProcessBatchQueue(),
             data_mover.SCPPush(),
             data_mover.SCPPull(),
             checksum.AddChecksum(),
             checksum.CompareChecksums(),
             clear.AlertFailedTransfer(),
             clear.ClearDAQBuffer()
             ]

    while True:
        for task in tasks:
            logging.info("Executing %s." % task.__class__.__name__)
            task.go()

        # Decide to continue or not
        if run_once:
            break
        else:
            logging.info('Sleeping.')
            time.sleep(60)


if __name__ == '__main__':
    main()
