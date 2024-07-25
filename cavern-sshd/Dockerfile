FROM almalinux:8

RUN \
  dnf install -y authconfig authselect openldap-clients openssh-clients openssh-server procps-ng sssd-client sssd-ldap sssd-tools && \
  touch /run/utmp

RUN mkdir -p /var/log/sssd

# enable sssd-based authentication
RUN authconfig --update --enablesssd --enablesssdauth

ADD src/nsswitch.conf /etc/

COPY src/entrypoint /entrypoint
COPY src/cadc-sssd-start /usr/bin/cadc-sssd-start

EXPOSE 22

ENTRYPOINT [ "/entrypoint" ]
CMD ["/usr/sbin/sshd", "-D", "-e"]
