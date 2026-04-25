# /// script
# dependencies = [
#     "altair",
#     "databricks-sdk==0.105.0",
#     "marimo",
#     "polars==1.40.1",
#     "python-dotenv==1.2.2",
#     "wigglystuff==0.3.5",
# ]
# requires-python = ">=3.13"
# ///

import marimo

__generated_with = "0.23.3"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    from databricks.sdk import WorkspaceClient
    from os import getenv
    from dotenv import load_dotenv
    from wigglystuff import EnvConfig

    load_dotenv(dotenv_path=".env")

    DBRICKS_HOST = getenv("***REMOVED***")
    DBRICKS_CLIENT_ID = getenv("***REMOVED***")
    DBRICKS_API_KEY = getenv("***REMOVED***")
    return (
        DBRICKS_API_KEY,
        DBRICKS_CLIENT_ID,
        DBRICKS_HOST,
        EnvConfig,
        WorkspaceClient,
        mo,
    )


@app.cell
def _(
    DBRICKS_API_KEY,
    DBRICKS_CLIENT_ID,
    DBRICKS_HOST,
    EnvConfig,
    WorkspaceClient,
    mo,
):
    def check_key(api_key: str):
        w = WorkspaceClient(
            host=DBRICKS_HOST,
            client_id=DBRICKS_CLIENT_ID,
            client_secret=DBRICKS_API_KEY,
        )
        return w.clusters.list()


    env_vars_valid = mo.ui.anywidget(
        EnvConfig(
            variables={
                "***REMOVED***": None,
                "***REMOVED***": None,
                "***REMOVED***": check_key,
            }
        )
    )

    env_vars_valid
    return (env_vars_valid,)


@app.cell
def _(env_vars_valid):
    env_vars_valid.require_valid()
    return


@app.cell
def _(DBRICKS_API_KEY, DBRICKS_CLIENT_ID, DBRICKS_HOST, WorkspaceClient):
    w = WorkspaceClient(
        host=DBRICKS_HOST,
        client_id=DBRICKS_CLIENT_ID,
        client_secret=DBRICKS_API_KEY,
    )
    print(w.workspace.list("/Users/mypythtesting@gmail.com/nhs-pipeline-demo"))
    return


if __name__ == "__main__":
    app.run()
