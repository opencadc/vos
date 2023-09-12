# Mountable, filesystem-based VOSpace-2.1 service (cavern)
`cavern` is a VOSpace 2.1 compliant service where both the data and metadata are based on an underlying file system.  
The permissions, too, are governed by the file system permissions.  For the users and groups in the file system 
to correspond to the users in the VOSpace REST API, a SSSD connection is made from the file system to LDAP.  
An access control service (https://github.com/opencadc/ac.git) is used by `cavern` to determine Posix UIDs and GIDs.  
It needs to be available and connected to the same LDAP instance as SSSD.

Because the source of all data and metadata in `cavern` is the file system, users may interact with that file system 
directly (through, for example, a volume mount) and `cavern` will reflect any changes that were made. 

## deployment
The `cavern` war file can be renamed at deployment time in order to support an alternate service name, including 
introducing additional path elements. 
See <a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a> (war-rename.conf).

For the SSS connection to LDAP to operate, the directory `/var/lib/sss/pipes` must be mapped to
the same `/var/lib/sss/pipes` directory seen by the SSS daemon, either through a volume mount or some other means.  
See <a href="https://github.com/opencadc/ac/tree/master/cadc-sssd">cadc-sssd</a> for more information.

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
org.opencadc.cavern.filesystem.root = {directory path}

# properties for file system probing and testing
org.opencadc.cavern.filesystem.probe.root = {directory path}
org.opencadc.cavern.filesystem.probe.owner = {username}
org.opencadc.cavern.filesystem.probe.linkOwner = {username}

# base directory exposed for sshfs mounts
org.opencadc.cavern.sshfs.serverBase = {server}[:{port}]:{path}
```

The _resourceID_ is the resourceID of _this_ `cavern` service.

The _filesystem.root_ is the path to a root directory containing the `cavern` directories and files.

The _filesystem.probe.root_ is the path to a directory used to test that `cavern` can perform all the required file system operations. 

The _filesystem.probe.owner_ is the user who owns the files and directories in the _filesystem.probe.root_ directory.

The _filesystem.probe.linkOwner_ is the user who owns the symbolic links in the _filesystem.probe.root_ directory.

The _sshfs.serverBase_ is the host name, port, and path to the sshfs mount.

### cavern-availability.properties (optional)
The `cavern-availability.properties` file specifies which users have the authority to change the availability state of 
the `cavern` service. Each entry consists of a key=value pair. The key is always "users". The value is the x500 canonical user name.

Example:
```
users = {user identity}
```
`users` specifies the user(s) who are authorized to make calls to the service. The value is a list of user identities
(X500 distinguished name), one line per user. Optional: if the `cavern-availability.properties` is not found or does not
list any `users`, the service will function in the default mode (ReadWrite) and the state will not be changeable.

## building
```
> gradle clean build

> docker build -t images.canfar.net/skaha-system/cavern:latest -f Dockerfile .
```
Then, update VERSION with the appropriate tags and run ./apply-version.sh

## running it
```
docker run -d --volume=/path/to/external/config:/config:ro --name cavern-tomcat images.canfar.net/skaha-system/cavern:latest
```
