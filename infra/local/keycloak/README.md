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

  - "Keycloak Client 1: Lakekeeper" -  Public client for interactive, browser-based authentication. `client_id = lakekeeper`
  - "Keycloak Client 2: Machine User" - Machine client for non-interactive machine users. `client_id=localinfra`

Once these steps are complete export the realm to JSON:

- Click "Realm settings" in the left-hand and then click "Action" -> Partial export" in the top right.
- On the "Partial export" screen ensure "Include clients" is checked then click "Export".
  A file called "realm-export.json" will be downloaded.
- Move the downloaded file to the same directory as this file and rename it to `realm-iceberg.json`.
- Open the file and search for the string `"clientId": "localinfra"`. In the same JSON dict find the
  key named `secret`; it's value should currently be `\*\*\*\*\*\*\*\*\*\*`.
  Change the value to `s3cr3t`.**DO NOT DO THIS IN PRODUCTION!**

Once the file is created and secret value changed run `docker compose down` to stop the services.
