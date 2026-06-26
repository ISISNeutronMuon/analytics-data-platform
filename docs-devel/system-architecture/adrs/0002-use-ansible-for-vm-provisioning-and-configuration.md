# 2. Use Ansible for VM provisioning and configuration

Date: 2024-07-03

## Status

Accepted

## Context

We need a set of VMs to hold all of the services for the project.

## Decision

Use [Ansible](https://docs.ansible.com/) to automate creation and configuration
of all of the nodes rather than [Kubernetes](https://kubernetes.io/).

## Consequences

This is a proof-of-concept system and does not need to be production grade in terms
of scaling and resiliency. This runs the risk that even the proof-of-concept system
is not sufficiently resilient even for the small number of test users.
