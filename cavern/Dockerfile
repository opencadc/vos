FROM images.opencadc.org/library/cadc-tomcat:1

## cavern specific
RUN dnf install -y sssd-client acl && dnf clean all

# cavern needs:
# - sssd-client 
# - acl
# but they are already installed in cadc-tomcat so nothing to do

COPY build/libs/cavern.war /usr/share/tomcat/webapps/

