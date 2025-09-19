# Keycloak realms

[Keycloak](https://www.keycloak.org/) uses
[realms](https://docs.redhat.com/en/documentation/red_hat_build_of_keycloak/26.0/html/server_administration_guide/red_hat_build_of_keycloak_features_and_concepts#core_concepts_and_terms)
to manage sets of users for authentication. By default a `master` realm is created that has complete
access to all Keycloak configuration, including creating new realms.

The JSON-file, [realm-iceberg.json](./realm-iceberg.json), provides a basic configuration for the purposes
of the local docker-composed based setup. The file was created in Keycloak and exported using the
following steps (the working directory should be the parent directory of this file):

- Run `docker compose up traefik keycloak -d`
- Visit http://analytics.localhost:58080/auth and enter `adpuser`/`adppassword` as the credentials.
