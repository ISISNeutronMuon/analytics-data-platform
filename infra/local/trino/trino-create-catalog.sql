create catalog if not exists playground using iceberg
with (
  "iceberg.catalog.type" = 'rest',
  "iceberg.rest-catalog.warehouse" = 'playground',
  "iceberg.rest-catalog.uri" = 'http://traefik/iceberg/catalog',
  "iceberg.rest-catalog.vended-credentials-enabled" = 'false',
  "iceberg.rest-catalog.security" = 'OAUTH2',
  "iceberg.rest-catalog.oauth2.server-uri" = 'http://traefik/auth/realms/iceberg/protocol/openid-connect/token',
  "iceberg.rest-catalog.oauth2.credential" = 'localinfra:s3cr3t',
  "iceberg.rest-catalog.oauth2.scope" = 'lakekeeper offline_access',
  "fs.native-s3.enabled" = 'true',
  "s3.endpoint" = 'http://minio:59000',
  "s3.region" = 'local-01',
  "s3.path-style-access" = 'true',
  "s3.aws-access-key" = 'adpuser',
  "s3.aws-secret-key" = 'adppassword'
);
