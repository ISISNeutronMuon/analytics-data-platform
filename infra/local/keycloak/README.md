# Keycloak realms

[Keycloak](https://www.keycloak.org/) uses
[realms](https://docs.redhat.com/en/documentation/red_hat_build_of_keycloak/26.0/html/server_administration_guide/red_hat_build_of_keycloak_features_and_concepts#core_concepts_and_terms)
to manage sets of users for authentication. By default a `master` realm is created that has complete
access to all Keycloak configuration, including creating new realms.

The JSON-file, [realm-iceberg.json](./realm-iceberg.json), provides a basic configuration for the purposes
of the local docker-composed based setup. The file was created in Keycloak and exported using the
following steps (the working directory should be the parent directory of this file):

- Run `docker compose up traefik keycloak -d
- Visit http://analytics.localhost:58080/auth and enter `adpuser`/`adppassword` as the credentials.
  If you see "Bad Gateway" then wait a few seconds and refresh the page (it can be slow to start).
- Click "Manage realms" -> "Create realm", enter "iceberg" as the realm name and click "Create".
- Now follow the instructions in the [Keycloak section](https://docs.lakekeeper.io/docs/nightly/authentication/#keycloak)
  of the Lakekeeper authentication guide to create two clients:

  - Public client for interactive, browser-based authentication
  - Machine client for non-interactive machine users.

- Once these steps are complete export the realm to JSON in the "Manage realms" menu.
