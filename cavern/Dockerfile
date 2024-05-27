FROM images.opencadc.org/library/cadc-tomcat:1

## cavern specific
RUN dnf install -y acl attr && dnf clean all

COPY build/libs/cavern.war /usr/share/tomcat/webapps/
