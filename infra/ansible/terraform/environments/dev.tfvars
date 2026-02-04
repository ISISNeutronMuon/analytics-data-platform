# general
vm_user = "ubuntu"
vm_key_pair = "martyngigg"
vm_image = "ubuntu-noble-24.04-nogui"

# networking
network_name        = "lakehouse-dev"
network_subnet_cidr = "192.168.44.0/24"
external_network_id = "5283f642-8bd8-48b6-8608-fa3006ff4539"
security_groups = {
  # 80 & 443 already exist as default groups in the cloud
  "traefik-dev" = {
    ports_ingress = [8443, 9000]
  },
  "datastore-dev" = {
    ports_ingress = [5432, 9000, 9001]
  }
  "keycloak-dev" = {
    ports_ingress = [8080, 9000]
  },
  "lakekeeper-dev" = {
    ports_ingress = [8181]
  },
  "trino-dev" = {
    ports_ingress = [8060]
  },
  "superset_farm-dev" = {
    ports_ingress = [8088]
  }
}

# compute
instance_configs = {
  "traefik-dev" = {
    flavor_name                = "l3.nano"
    ansible_group              = "traefik"
    additional_security_groups = ["HTTP", "HTTPS", "traefik-dev"]
  },
  "datastore-dev" = {
    flavor_name                = "l3.nano"
    ansible_group              = "datastore"
    additional_security_groups = ["datastore-dev"]
  },
  "keycloak-dev" = {
    flavor_name                = "l3.nano"
    ansible_group              = "keycloak"
    additional_security_groups = ["keycloak-dev"]
  },
  "lakekeeper-dev" = {
    flavor_name                = "l3.nano"
    ansible_group              = "lakekeeper"
    additional_security_groups = ["lakekeeper-dev"]
  },
  "trino-dev" = {
    flavor_name                = "l3.nano"
    ansible_group              = "trino"
    additional_security_groups = ["trino-dev"]
  },
  "superset_farm-dev" = {
    flavor_name                = "l3.nano"
    ansible_group              = "superset_farm"
    additional_security_groups = ["superset_farm-dev"]
  }
  "elt-dev" = {
    flavor_name                = "l3.nano"
    ansible_group              = "elt"
  }
}
floating_ip_info = {
  vm_name     = "traefik-dev"
  floating_ip = "130.246.81.245"
}

# ansible
ansible_inventory_filename = "inventory-dev.ini"
