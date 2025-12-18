# Troubleshooting

Here we capture a collection of common issues when dealing with deployment.

## Ansible stuck waiting 'TASK [vm_create : Wait for connection]'

This can happen when a new node has been created with an IP address matching
an old node. Remove all references of the offending IP address
from `~/.ssh/known_hosts`: `sed -i ""  -E -e '/^<ip_address>/d' ~/.ssh/known_hosts`
(where `<ip_address>` should be replaced with the real value).

This can also happen when recreating the Traefik node and the floating IP now
points at a different node. Again, remove the IP address from `~/.ssh/known_hosts`.
