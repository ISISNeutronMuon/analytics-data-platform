# 3. Use SCD Cloud VMs

Date: 2024-07-04

## Status

Accepted

## Context

Access to compute is required to build the lakehouse proof of concept.
It will be beneficial to be able to create/destroy resources on demand.

## Decision

ISIS already has significant resource available within the [SCD Cloud](https://openstack.stfc.ac.uk),
available at no additional cost.
The system will be built using these resources.

## Consequences

For now, ISIS has a healthy resource allocation in the cloud and these can be created
on demand.
The cloud has had a period of instability recently and as such a risk remains as
to whether it can offer the reliability required to host this system.
