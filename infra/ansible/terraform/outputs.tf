output "ansible_inventory" {
  description = "The full path to the generated Ansible inventory"
  value       = local_file.ansible_inventory.filename
}
