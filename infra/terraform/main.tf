terraform {
  required_version = ">= 0.14.0"
  required_providers {
    openstack = {
      source  = "terraform-provider-openstack/openstack"
      version = "~> 3.4.0"
    }
  }
}

provider "openstack" {
  cloud = var.cloud_name
}

# networking
resource "openstack_networking_network_v2" "network" {
  name           = var.network_name
  external       = false
  admin_state_up = true
}

resource "openstack_networking_subnet_v2" "subnet" {
  name        = "${var.network_name}-1"
  network_id  = openstack_networking_network_v2.network.id
  cidr        = var.network_subnet_cidr
  ip_version  = 4
  enable_dhcp = false
}

resource "openstack_networking_router_v2" "router" {
  name                = "${var.network_name}-router"
  admin_state_up      = true
  external_network_id = var.external_network_id
}

resource "openstack_networking_router_interface_v2" "router_interface" {
  router_id = openstack_networking_router_v2.router.id
  subnet_id = openstack_networking_subnet_v2.subnet.id
}

resource "openstack_networking_secgroup_v2" "sec_group" {
  for_each = var.security_groups

  name        = each.key
  description = "Ingress ports for ${each.key}"
}

locals {
  sec_groups_map = merge([
    for sg_name, sg_config in var.security_groups : {
      for port in sg_config.ports_ingress : "${sg_name}-${port}" => {
        sec_group_id = openstack_networking_secgroup_v2.sec_group[sg_name].id
        port_ingress = port
      }
    }
  ]...)
}

resource "openstack_networking_secgroup_rule_v2" "secgroup_ingress" {
  for_each = local.sec_groups_map

  direction         = "ingress"
  ethertype         = "IPv4"
  protocol          = "tcp"
  port_range_min    = each.value.port_ingress
  port_range_max    = each.value.port_ingress
  remote_ip_prefix  = "0.0.0.0/0"
  security_group_id = each.value.sec_group_id
}

# compute
resource "openstack_compute_instance_v2" "vm" {
  for_each = var.instance_configs

  name            = each.key
  flavor_name     = each.value.flavor_name
  image_name      = each.value.image_name
  key_pair        = each.value.key_pair
  security_groups = concat(["default"], each.value.additional_security_groups)

  network {
    name = var.network_name
  }

  depends_on = [openstack_networking_subnet_v2.subnet]
}

data "openstack_networking_port_v2" "vm_port" {
  device_id  = openstack_compute_instance_v2.vm[var.floating_ip_info.vm_name].id
  depends_on = [openstack_compute_instance_v2.vm]
}

resource "openstack_networking_floatingip_associate_v2" "vm_fip" {
  floating_ip = var.floating_ip_info.floating_ip
  port_id     = data.openstack_networking_port_v2.vm_port.id
}

resource "local_file" "ansible_inventory" {
  content = templatefile("ansible-inventory.tmpl",
    {
      vm_user            = var.vm_user
      python_interpreter = var.python_interpreter_path
      floating_ip        = var.floating_ip_info.floating_ip
      ansible_groups     = [for vm in openstack_compute_instance_v2.vm : vm.name]
      ansible_ips        = [for vm in openstack_compute_instance_v2.vm : vm.access_ip_v4]
    }
  )
  filename = "${var.ansible_inventory_filename}"
}
