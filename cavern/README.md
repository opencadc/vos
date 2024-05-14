# Mountable, filesystem-based VOSpace-2.1 service (cavern)
`cavern` is a VOSpace 2.1 compliant service where both the data and metadata are based on an underlying file system.  
The permissions, too, are governed by the file system permissions. 
An access control service (https://github.com/opencadc/ac.git) is used by `cavern` to determine Posix UIDs and GIDs.  
The UIDs and GIDs come from an external source (TBD).

Because the source of all data and metadata in `cavern` is the file system, users may interact with that file system 
directly (through, for example, a volume mount) and `cavern` will reflect any changes that were made. 

IMPORTANT: `cavern` acts as a file server and creates files and directories owned by other users; it _must_
be run as root rather than the usual unprivileged `tomcat` user.

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

`cavern` requires an external service to provide mapping of user and group names to local POSIX uid and gid values
using the `PosixMapperClient` in the <a href="https://github.com/opencadc/ac/tree/master/cadc-gms">cadc-gms</a> library.

### cavern.properties
A `cavern.properties` file in /config is required to run this service.  The following keys are required:
```
# service identity
org.opencadc.cavern.resourceID = ivo://{authority}/{name}

# (optional) identify which container nodes are allocations
org.opencadc.cavern.allocationParent = {top level node}

# (optional) provide a class implementing the org.opencadc.cavern.nodes.QuotaPlugin interface to control Quotas
# for users.
# Optional, but the default of NoQuotaPlugin will get used if not specified, and users will have free reign.
#
org.opencadc.cavern.nodes.QuotaPlugin = {plugin implementation class}

# base directory for cavern files
org.opencadc.cavern.filesystem.baseDir = {persistent data directory in container}
org.opencadc.cavern.filesystem.subPath = {relative path to the node/file content that could be mounted in other containers}

# owner of root node has admin power
org.opencadc.cavern.filesystem.rootOwner = {username}

# (optional) username, uid, and gid (default) of the root owner
org.opencadc.cavern.filesystem.rootOwner.username = {username}
org.opencadc.cavern.filesystem.rootOwner.uid = {uid}
org.opencadc.cavern.filesystem.rootOwner.gid = {gid}

# (optional) keys to generate pre-auth URLs to cavern: now generated internally

# (optional) base directory exposed for sshfs mounts
org.opencadc.cavern.sshfs.serverBase = {server}[:{port}]:{path}
```

The _resourceID_ is the resourceID of _this_ `cavern` service.

The _allocationParent_ is a path to a container node (directory) which contains space allocations. An allocation
is owned by a user (usually different from the _rootOwner_ admin user) who is responsible for the allocation
and all content therein. The owner of an allocation is granted additional permissions within their 
allocation (they can read/write/delete anything) so the owner cannot be blocked from access to any content
within their allocation. This probably only matters for multi-user projects. Multiple _allocationParent_(s) may
be configured to organise the top level of the content (e.g. /home and /projects). Paths configured to be 
_allocationParent_(s) will be automatically created (if necessary), owned by the _rootOwner_, and will be
anonymously readable (public). Limitation: only top-level container nodes can be configured as _allocationParent_(s).

The _filesystem.baseDir_ is the path to a base directory containing the `cavern` nodes/files.

The _filesystem.subPath_ is the relative path to the node/file content that could be mounted in other containers.

The _filesystem.rootOwner_ is the username of the owner of the root container in the VOSpace. The root owner has some admin
privileges: can create allocations (create a container node owned by another user) and can set the quota property
on such containers. Note: quota is not currently implemented in `cavern`.

The _org.opencadc.cavern.nodes.QuotaPlugin_ is the concrete class that implements the 
[QuotaPlugin](./src/main/java/org/opencadc/cavern/nodes/QuotaPlugin.java) interface.  Absences of this property 
assumes no quota support and users can fill underlying storage in an uncontrolled way.

The `cavern` service must be able to resolve the root owner username to a POSIX uid and gid pair during startup. If
the configured IdentityManager does not support privileged access to user info, the correct values must be configured 
using the optional _filesystem.rootOwner.uid_ and _filesystem.rootOwner.gid_ properties.

NOT FUNCTIONAL: The optional _sshfs.serverBase_ is the host name, port, and path to the sshfs mount of the `cavern` content. Clients
can negotiate the mount of a specific container node in the VOSpace and be given back a connection string suitable for
use with the `sshfs` command. This requires a separate sshd running with the same A&A system and mounting the same
underlying filesystem. See <a href="https://github.com/opencadc/vos/tree/master/cavern-sshd">cavern-sshd</a> for an
example of the legacy cavern SSHD support, but note that this won't work out of the box due to the use of `sssd` for
for auth.

### cadcproxy.pem (optional)
This client certificate may be required to make authenticated server-to-server calls for system-level A&A purposes.
Specifically, this is needed for the CADC access-control system support and can be used to call a local
`posix-mapper` service to map between uid/gid(s) and username/groupname(s).

## building
```
gradle clean build

docker build -t cavern:latest -f Dockerfile .
```
## checking it
```
docker run --rm -it cavern:latest /bin/bash
```
## running it
```
docker run -d \
--user root:root \
--volume=/path/to/data/volume:/{org.opencadc.cavern.filesystem.baseDir}:rw \
--volume=/path/to/external/config:/config:ro \
--name cavern cavern:latest
```
