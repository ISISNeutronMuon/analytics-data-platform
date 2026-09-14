\getenv openfga_db_name OPENFGA_DB_NAME
\getenv lakekeeper_db_name LAKEKEEPER_DB_NAME
\getenv airflow_db_name AIRFLOW_DB_NAME

-- Core service databases
CREATE DATABASE :"openfga_db_name";
CREATE DATABASE :"lakekeeper_db_name";

-- Optional service databases. Strictly not required by the core
-- but it's easier to create them here.
CREATE DATABASE :"airflow_db_name";
