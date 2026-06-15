# TLS Certificates

SSL/TLS certificates are provided by Digitial Infrastructure and stored in the Vault.

A [certificate signing request](https://en.wikipedia.org/wiki/Certificate_signing_request)
is required to request a new certificate. To generate a signing request
use the Ansible playbook [site_csr.yml](../../infra/ansible/site_csr.yml):

```sh
> cd infra/ansible
> ansible-playbook -i inventories/[ENV]/inventory-[ENV].ini -e csr_out_path=$PWD/domain.csr
```

where `[ENV]` should be replaced with the environment the certificate is targeted at. Look at the
directories available in `infra/ansible/inventories` for the options.

Check the contents using:

```sh
> openssl req -in $PWD/domain.csr -text -noout
```

Copy the content of the `.csr` file and send it to Digital Infrastructure. The request can take a
few days to process. When the new certificates arrive there will be several format options to choose
from, select "Certificate (w/ issuer after), PEM encoded". Download this, copy the contents to the
Vault and then redeploy Traefik playbook to copy the new certificates over.
