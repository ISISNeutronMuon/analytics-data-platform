# Certificates

The subdirectories contain the certificate request configurations for the
project, by domain. Each subdirectory is named as `<domain_name>/YYYY-MM` where `YYYY-MM`
is the date that the request was made.

The keys and certificates are stored in a shared folder in Keeper and the latest
is stored in the Ansible vault.

## Generate a new certificate request

- Make a new directory named using the current year/month in the format above.
- Copy the previous certificate request configuration file (*.cnf) to the new directory

The next step depends on whether you already have a key. If you have an existing key, run:

```sh
openssl req -nodes -new -key [SERVER_NAME].key -out [SERVER_NAME].csr -config [SERVER_NAME].cnf
```

If you need to generate a new key, run:

```sh
openssl req -nodes -new -newkey rsa:4096 -keyout [SERVER_NAME].key -out [SERVER_NAME].csr -config [SERVER_NAME].cnf
```

where `[SERVER_NAME]` should be replaced by the domain name of the server.

## Installing the new certificate chain

The certificate authority will return the certificate chain in a variety of different formats and
combinations. For use with our Traefik proxy select the "Certificate (w/ issuer after), PEM encoded"
bundle. Update the relevant Ansible vault with the new content.
