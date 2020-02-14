# Cavern - A mountable, filesystem-based VOSpace implementation

Cavern is a VOSpace 2.1 compliant service where both the data and metadata are based on an underlying file system.  The permissions, too, are governed by the file system permissions.  For the users and groups in the file system to correspond to the users in the VOSpace REST API, a SSSD connection is made from the file system to LDAP.  An access control service, used by VOSpace to determine Posix UIDs and GIDs, needs to be run and connected to the same LDAP instance as SSSD.

Because the source of all data and metadata in Cavern is the file system, users may interact with that file system (through, for example, a volume mount) and have have VOSpace access to that same data and metadata. 

## building

```
gradle clean build
docker build -t cavern-tomcat -f Dockerfile .
```

## checking it
```
docker run -it cavern-tomcat:latest /bin/bash
```

## running it
```
docker run -d --volume=/path/to/external/config:/config:ro --volume=/path/to/external/logs:/logs:rw --name cavern-tomcat cavern-tomcat:latest
```

## configuration

See the <a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a> image docs 
for expected deployment and general config requirements.

The following files are expected to be in /path/to/external/config:
* Cavern.properties - see the version in this directory as an example
* LocalAuthority.properties

For the SSS connection to LDAP to operate, the directory /var/lib/sss/pipes must be mapped to the same /var/lib/sss/pipes directory seen by the SSS daemon, either through a volume mount or some other means.  See the SSSD container in https://github.com/ac.git for more information.