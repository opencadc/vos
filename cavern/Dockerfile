FROM cadc-tomcat:latest

## cavern specific

# add often used tools
RUN dnf -y install which

# needed to get/set quota
RUN dnf -y install attr

# cavern needs:
# - sssd-client 
# - acl
# but they are already installed in cadc-tomcat so nothing to do

COPY build/libs/cavern.war /usr/share/tomcat/webapps/

