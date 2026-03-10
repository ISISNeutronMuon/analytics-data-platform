import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    from pyiceberg.catalog import load_catalog


    def catalog_connect(warehouse: str):
        return load_catalog(
            warehouse,
            **{
                "type": "rest",
                "uri": "http://adp-router:50080/iceberg/catalog",
                "credential": "machine-infra:s3cr3t",
                "oauth2-server-uri": "http://adp-router:50080/auth/realms/analytics-data-platform/protocol/openid-connect/token",
                "scope": "lakekeeper",
                "warehouse": warehouse,
            },
        )


    playground = catalog_connect("playground")
    playground_landing = catalog_connect("playground_landing")
    return (playground_landing,)


@app.cell
def _(playground):
    playground.list_namespaces()
    return


if __name__ == "__main__":
    app.run()
