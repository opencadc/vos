# sshd container for cavern file system mounting

## building

```
docker build -t cavern-sshd:latest -f Dockerfile .
```

## configuration

/etc/sshd/sshd_conf controls the configuration and should be overwritten with the desired version with a file system mount.  The field ChrootDirectory should specify the root of the file system which to expose.  (eg /cavern).  This must match the value of `org.opencadc.cavern.filesystem.baseDir` in the `cavern.properties`.

The public and private keys for the supported cypher algorithms must also exist in directory /etc/sshd/:

```
ssh_host_ecdsa_key
ssh_host_ed25519_key
ssh_host_rsa_key
ssh_host_ecdsa_key.pub
ssh_host_ed25519_key.pub
ssh_host_rsa_key.pub
```
