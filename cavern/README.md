# Cavern - A mountable, filesystem-based VOSpace implementation

Cavern is a VOSpace 2.1 compliant service where both the data and metadata are based on an underlying file system.  The permissions, too, are governed by the file system permissions.  For the users and groups in the file system to correspond to the users in the VOSpace REST API, a SSSD connection is made from the file system to LDAP.  An access control service (https://github.com/opencadc/ac.git) is used by VOSpace to determine Posix UIDs and GIDs.  It needs to be available and connected to the same LDAP instance as SSSD.

Because the source of all data and metadata in Cavern is the file system, users may interact with that file system directly (through, for example, a volume mount) and cavern will reflect any changes that were made. 

## building
```
> gradle clean build

> docker build -t images.canfar.net/skaha-system/cavern:latest -f Dockerfile .
```
Then, update VERSION with the approripate tages and run ./apply-version.sh

## running it
```
docker run -d --volume=/path/to/external/config:/config:ro --name cavern-tomcat images.canfar.net/skaha-system/cavern:latest
```

## configuration

See the <a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a> image docs 
for expected deployment and general config requirements.

Runtime configuration must be made available via the `/config` directory, including
the following files:
* Cavern.properties
* LocalAuthority.properties

For the SSS connection to LDAP to operate, the directory /var/lib/sss/pipes must be mapped to the same /var/lib/sss/pipes directory seen by the SSS daemon, either through a volume mount or some other means.  See the access control git hub project mentioned above for more information.


### catalina.properties

```
# database connection pools
org.opencadc.cavern.uws.maxActive={max connections for jobs pool}
org.opencadc.cavern.uws.username={database username for jobs pool}
org.opencadc.cavern.uws.password={database password for jobs pool}
org.opencadc.cavern.uws.url=jdbc:postgresql://{server}/{database}
```

The `uws` pool manages (create, alter, drop) uws tables and manages the uws content (creates and modifies jobs in the uws
schema when jobs are created and executed by users.