# Imports
import logging
from os import walk, path
from os.path import join
from swiftclient.multithreading import OutputManager
from swiftclient.service import SwiftService, SwiftError, SwiftUploadObject

logging.basicConfig(level=logging.ERROR)
logging.getLogger("requests").setLevel(logging.CRITICAL)
logging.getLogger("swiftclient").setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)

# Account information
authVersion = '1.0'
authURL = 'http://192.168.244.10/auth/v1.0'
user = 'test:tester'
key = 'testing'
uu_threads = 20
authDict = {"auth_version": authVersion, "auth": authURL, "user": user, "key": key, "object_uu_threads": uu_threads}

# TODO - make directoryVar an argument on the command line
directoryVar = '/Users/jimm/A_Test'

# TODO - make the containerVar an argument on the command line
containerVar = 'container2'

with SwiftService(authDict) as swift, OutputManager() as out_manager:
    try:
        # Collect all the files and folders in the given directory
        objsVar = []
        dir_markers = []
        for (_dir, _ds, _fs) in walk(directoryVar):
            if not (_ds + _fs):
                dir_markers.append(_dir)
            else:
                objsVar.extend([join(_dir, _f) for _f in _fs])

        # Now that we've collected all the required files and dir markers
        # build the ``SwiftUploadObject``s for the call to upload
        objsVar = [
            SwiftUploadObject(
                o, object_name=path.relpath(o, directoryVar)
            ) for o in objsVar
            ]
        dir_markers = [
            SwiftUploadObject(
                d, object_name=path.relpath(d, directoryVar), options={'dir_marker': True}
            ) for d in dir_markers
        ]

        # DEBUG --  uncomment to stop program processing at this point
        # exit()

        # Schedule uploads on the SwiftService thread pool and iterate
        # over the results
        for r in swift.upload(containerVar, objsVar + dir_markers):     # Upload happens here!
            if r['success']:                                # If successfully uploaded
                if 'object' in r:                           # and it's an object
                    print(r['object'])                      # print out the object name
                elif 'for_object' in r:                     # or if it is an object segment
                    print(                                  # print the relative crap for a segment
                        '%s segment %s' % (r['for_object'],
                                           r['segment_index'])
                        )
            else:
                error = r['error']                          # If not successful, tell me why
                if r['action'] == "create_container":
                    logger.warning(
                        'Warning: failed to create container '
                        "'%s'%s", containerVar, error
                    )
                elif r['action'] == "upload_object":
                    logger.error(
                        "Failed to upload object %s to container %s: %s" %
                        (containerVar, r['object'], error)
                    )
                else:
                    logger.error("%s" % error)

    except SwiftError as e:
        logger.error(e.value)
