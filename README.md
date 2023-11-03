# vos
client and server implementation of VOSpace 2.1 specification

## version 2.x API

The main branch now contains version 2.0 or later of the main vos libraries. There are
many source-incompatible changes from the 1.x versions. The 2.x series includes code
in the `org.opencadc.vospace` package (and sub packages).


## version 1.x API

The `vos1` branch contains the latest 1.x libray code; it will be maintained until the
last applications using it are ported to 2.x version.

## VOSpace services

`cavern` is a POSIX filesystem based VOSpace service updated to the 2.x API (this repo).

`vault` is an object-store based VOSpace service based on the 2.x APIs and designed to
work with OpenCADC Storage Inventory as the back end storage system 
(<a href="https://github.com/opencadc/storage-inventory">storage-inventory</a> repo).
