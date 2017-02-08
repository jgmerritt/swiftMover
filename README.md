# swiftMover
This project was created in order to move large amounts of data from a mounted CIFS fileshare to 
an openstack swift container. My definition of large is ~50TB fileshare with less than 1 million
files.

This is currently VERY MUCH a WORK in PROGRESS.

## Prerequisites
This project mainly uses the openstack swift swiftclient python API. Specifically the 
swiftclient.service method.
