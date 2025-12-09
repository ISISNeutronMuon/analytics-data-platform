import marimo

__generated_with = "0.16.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import os

    import dotenv
    import marimo as mo
    from pyiceberg.catalog.rest import RestCatalog

    dotenv.load_dotenv(".env")
    return RestCatalog, os


@app.cell
def _(RestCatalog, os):
    # Connect to catalog
    accel_catalog = RestCatalog(
        name="accelerator",
        warehouse="accelerator",
        uri="https://analytics.isis.cclrc.ac.uk/iceberg/catalog",
        token=os.environ["CATALOG_TOKEN"]
    )
    return (accel_catalog,)


@app.cell
def _(accel_catalog):
    downtime = accel_catalog.load_table("analytics_operations.mcr_equipment_downtime_records")
    return (downtime,)


@app.cell
def _(downtime):
    df = downtime.scan(row_filter="cycle_name = '2025/1'").to_pandas()
    return (df,)


@app.cell
def _(df):
    df
    return


if __name__ == "__main__":
    app.run()
