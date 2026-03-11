# Troubleshooting

Here we capture a collection of common issues when dealing with deployment and local development.

## Deployment

### Ansible cannot connect via ssh'

This can happen when a new node has been created with an IP address matching
an old node. Remove all references of the offending IP addresses from
`~/.ssh/known_hosts` with `ssh-keygen -R <ip_address>`, where `<ip_address>` should be replaced with the real value.
Be sure to do this for `198.` & `130.` addresses.

### Ansible Galaxy role errors

If Ansible fails with missing role errors, ensure you have installed the required Galaxy roles:

```bash
cd infra/ansible
uv run ansible-galaxy role install --roles-path galaxy_roles -r requirements-ansible-galaxy.yml
```

Also verify that your Python and Ansible versions match the project requirements in
`infra/ansible/requirements-python.txt`.

## Local development

### Docker Compose services fail to start or are slow

The local compose stack runs several services (Keycloak, Lakekeeper, Trino, MinIO, Postgres,
Superset, Traefik) and can be resource-heavy. Ensure Docker Desktop has sufficient resources
allocated — at least 4 CPU cores and 8 GB of memory is recommended.

If services fail with timeout errors, try increasing the wait timeout:

```bash
docker compose --profile superset up --wait --wait-timeout 300
```

### Cannot connect to services via `adp-router`

Verify the `/etc/hosts` entry is present:

```text
127.0.0.1    adp-router
```

If using a VPN or custom DNS, the entry may be overridden. Check with `ping adp-router`
to confirm it resolves to `127.0.0.1`.

### Trino SSL/TLS warnings

When running locally, you will see warnings like:

> InsecureRequestWarning: Unverified HTTPS request is being made to host 'localhost'.

This is expected in local development because Trino requires HTTPS and the local stack
uses a self-signed certificate. These warnings should not appear in production.
