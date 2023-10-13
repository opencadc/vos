FROM images.opencadc.org/library/cadc-tomcat:1

## cavern specific
RUN dnf install -y sssd-client acl && dnf clean all

COPY build/libs/cavern.war /usr/share/tomcat/webapps/
