FROM images.opencadc.org/library/cadc-tomcat:1

ENV IMAGE_VERSION="0.11.0"
LABEL org.opencontainers.image.version="${IMAGE_VERSION}"

# override default user
USER root:root

# required filesystem tools
RUN dnf install -y acl attr && dnf clean all

COPY build/libs/cavern.war /usr/share/tomcat/webapps/
