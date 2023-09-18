# Mountable, filesystem-based VOSpace-2.1 service (cavern)
`cavern` is a VOSpace 2.1 compliant service where both the data and metadata are based on an underlying file system.  
The permissions, too, are governed by the file system permissions. 
An access control service (https://github.com/opencadc/ac.git) is used by `cavern` to determine Posix UIDs and GIDs.  
The UIDs and GIDs come from an external source (TBD).

Because the source of all data and metadata in `cavern` is the file system, users may interact with that file system 
directly (through, for example, a volume mount) and `cavern` will reflect any changes that were made. 

## deployment
The `cavern` war file can be renamed at deployment time in order to support an alternate service name, including 
introducing additional path elements. 
See <a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a> (war-rename.conf).

## configuration
The following runtime configuration must be made available via the `/config` directory.

### catalina.properties
This file contains java system properties to configure the tomcat server and some of the java libraries 
used in the service.

See <a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a> for 
system properties related to the deployment environment.

See <a href="https://github.com/opencadc/core/tree/master/cadc-util">cadc-util</a> for common system properties.

`cavern` includes multiple IdentityManager implementations to support authenticated access:
- See <a href="https://github.com/opencadc/ac/tree/master/cadc-access-control-identity">cadc-access-control-identity</a> for CADC access-control system support.
- See <a href="https://github.com/opencadc/ac/tree/master/cadc-gms">cadc-gms</a> for OIDC token support.

`cavern` requires a connection pool to the local UWS database:
```
# database connection pools
org.opencadc.cavern.uws.maxActive={max connections for jobs pool}
org.opencadc.cavern.uws.username={database username for jobs pool}
org.opencadc.cavern.uws.password={database password for jobs pool}
org.opencadc.cavern.uws.url=jdbc:postgresql://{server}/{database}
```

The `uws` pool manages (create, alter, drop) uws tables and manages the uws content.

### cadc-registry.properties
See <a href="https://github.com/opencadc/reg/tree/master/cadc-registry">cadc-registry</a>.

### cavern.properties
A `cavern.properties` file in /config is required to run this service.  The following keys are required:
```
# service identity
org.opencadc.cavern.resourceID = ivo://{authority}/{name}

# base directory for cavern files
org.opencadc.cavern.filesystem.baseDir = {persistent data directory in container}
org.opencadc.cavern.filesystem.subPath = {relative path to the node/file content that could be mounted in other containers}

# (optional) keys to generate pre-auth URLs to cavern
org.opencadc.cavern.privateKey = {private key file name}
org.opencadc.cavern.publicKey = {public key file name}

# (optional) base directory exposed for sshfs mounts
org.opencadc.cavern.sshfs.serverBase = {server}[:{port}]:{path}
```

The _resourceID_ is the resourceID of _this_ `cavern` service.

The _filesystem.baseDir_ is the path to a base directory containing the `cavern` nodes/files.

The _filesystem.subPath_ is the relative path to the node/file content that could be mounted in other containers.

The optional _privateKey_ and _publicKey_ is used to sign pre-auth URLs (one-time token included in URL) so that 
a `cavern` service does not have to repeat permission checks. If the not set, cavern cannot generate pre-auth URLs, 
but it can generate plain URLs.

The optional _sshfs.serverBase_ is the host name, port, and path to the sshfs mount. 
See <a href="https://github.com/opencadc/vos/tree/master/cavern-sshd">cavern-sshd</a> for cavern SSHD support.

## building
```
gradle clean build

docker build -t cavern:latest -f Dockerfile .
```
Then, update VERSION with the appropriate tags and run ./apply-version.sh

## running it
```
docker run -d \
--user root:root \
--volume=/path/to/data/volume:/{org.opencadc.cavern.filesystem.baseDir}:rw \
--volume=/path/to/external/config:/config:ro \
--name cavern cavern:latest
```
