# general
vm_user = "ubuntu"
vm_key_pair = "martyngigg"
vm_image = "ubuntu-noble-24.04-nogui"

# networking
network_name        = "lakehouse-qa"
network_subnet_cidr = "192.168.43.0/24"
external_network_id = "5283f642-8bd8-48b6-8608-fa3006ff4539"
security_groups = {
  # 80 & 443 already exist as default groups in the cloud
  "traefik-qa" = {
    ports_ingress = [8443]
  },
  "keycloak-qa" = {
    ports_ingress = [8080, 9000]
  },
  "lakekeeper-qa" = {
    ports_ingress = [8181]
  },
  "trino-qa" = {
    ports_ingress = [8060]
  },
  "superset_farm-qa" = {
    ports_ingress = [8088]
  }
}

# compute
instance_configs = {
  "traefik-qa" = {
    flavor_name                = "l3.nano"
    ansible_group              = "traefik"
    additional_security_groups = ["HTTP", "HTTPS", "traefik-qa"]
  }
  "keycloak-qa" = {
    flavor_name                = "l3.micro"
    ansible_group              = "keycloak"
    additional_security_groups = ["keycloak-qa"]
  },
  "lakekeeper-qa" = {
    flavor_name                = "l3.xsmall"
    ansible_group              = "lakekeeper"
    additional_security_groups = ["lakekeeper-qa"]
  },
  "trino-qa" = {
    flavor_name                = "l3.xsmall"
    ansible_group              = "trino"
    additional_security_groups = ["trino-qa"]
  },
  "superset_farm-qa" = {
    flavor_name                = "l3.xsmall"
    ansible_group              = "superset_farm"
    additional_security_groups = ["superset_farm-qa"]
  }
  "elt-qa" = {
    flavor_name                = "l3.xsmall"
    ansible_group              = "elt"
  }
}
floating_ip_info = {
  vm_name     = "traefik-qa"
  floating_ip = "130.246.214.124"
}

# ansible
ansible_inventory_filename = "inventory-qa.ini"
