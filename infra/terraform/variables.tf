variable "cloud_name" {
  description = "Name of the cloud in the ~/.config/openstack/clouds.yaml file."
  type        = string
}

variable "vm_user" {
  description = "Name of the vm user automatically provisioned by the cloud and used by Ansible."
  type        = string
}

variable "python_interpreter_path" {
  description = "Path to the Python interpreter for Ansible."
  type        = string
  default     = "/usr/bin/python3"
}

variable "external_network_id" {
  description = "Id of the existing External network on the cloud"
  type        = string
}

variable "network_name" {
  description = "Name of the network"
  type        = string
}

variable "network_subnet_cidr" {
  description = "CIDR of network subnet"
  type        = string
}

variable "security_groups" {
  description = "Details of the ports to open on each compute instance"
  type = map(object({
    ports_ingress = list(number)
  }))
}

variable "instance_configs" {
  description = "Map of instance configurations where the key defines the name. The instance is attached to the network defined by 'network_name'"
  type = map(object({
    flavor_name                = string
    image_name                 = string
    key_pair                   = string
    additional_security_groups = optional(list(string))
  }))

}

variable "floating_ip_info" {
  description = "Define the instance that has the floating IP attached"
  type = object({
    vm_name     = string
    floating_ip = string
    }
  )
}

variable "ansible_inventory_filename" {
  description = "Filename for the generated ansible inventory"
  type        = string
}
