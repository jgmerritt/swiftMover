# Imports
import logging
from os import walk, path
from os.path import join
from swiftclient.multithreading import OutputManager
from swiftclient.service import SwiftService, SwiftError, SwiftUploadObject

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("requests").setLevel(logging.CRITICAL)
logging.getLogger("swiftclient").setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)
fh = logging.FileHandler('swiftDataMove.log')
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

# Account information
authVersion = '1.0'
authURL = 'https://brtnswiftdev.burton.com/auth/v1.0'
user = 'test'
key = 'testing'
uu_threads = 30
authDict = {"auth_version": authVersion, "auth": authURL, "user": user, "key": key, "object_uu_threads": uu_threads, "segment_size": 5368709120}

# TODO - make directoryVar an argument on the command line
directoryVar = '/home/jimm'

# TODO - make the containerVar an argument on the command line
containerVar = 'container3'

with SwiftService(authDict) as swift, OutputManager() as out_manager:
    try:
        # Collect all the files and folders in the given directory
        objsVar = []
        dir_markers = []
        for (_dir, _ds, _fs) in walk(unicode(directoryVar)):    # walk directory and force unicode decoding
            if not (_ds + _fs):
                dir_markers.append(_dir)                        # create list of pseudo directory objects
            else:
                if '.DS_Store' in _fs:
                    _fs.remove('.DS_Store')
                objsVar.extend([join(_dir, _f) for _f in _fs])  # create list of objects

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

        for _objs in objsVar:
            try:
                print _objs.object_name
            except UnicodeEncodeError:
                print "Failure to upload: ", _objs.source
                pass
        exit()

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
                        (r['object'], containerVar, error)
                    )
                else:
                    logger.error("%s" % error)

    except SwiftError as e:
        logger.error(e.value)
