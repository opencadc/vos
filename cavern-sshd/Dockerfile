FROM centos:7

RUN \
  yum -y install openssh-clients openssh-server sssd-client authconfig && \
  yum -y clean all && \
  touch /run/utmp

RUN mkdir -p /var/log/sshd

# enable sssd-based authentication
RUN authconfig --update --enablesssd --enablesssdauth

EXPOSE 22

CMD ["/usr/sbin/sshd", "-D", "-e"]
