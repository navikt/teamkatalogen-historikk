import os
import json
import logging
from google.cloud import secretmanager
from dbt.cli.main import dbtRunner, dbtRunnerResult


logging.basicConfig(level=logging.INFO)

def main():

    # henter secret for sørvisbrukeren historisere-teamkatalogen
    secret_name = 'serviceuser-bq-historisere-teamkatalogen'
    full_secret_name = f"projects/230094999443/secrets/{secret_name}/versions/latest"
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": full_secret_name})
    secret = json.loads(response.payload.data.decode("UTF-8"))
    for key, value in secret.items():
        key = 'DBT_ENV_SECRET_' + key
        os.environ[key] = value
        logging.info(f"Set environment variable {key}")

    # kjører dbt snapshot
    dbt_base_command = ["--log-format-file", "json"]
    dbt_command = ["snapshot"]
    dbt = dbtRunner()
    output: dbtRunnerResult = dbt.invoke(dbt_base_command + dbt_command)

    # etter kjørt dbt-kommando håndterer vi eventuell feil
    # exit code 2, feil utenfor dbt
    if output.exception:
        raise output.exception
    # exit code 1, feil i dbt under test eller kjøring
    if not output.success:
        raise Exception(output.result)


if __name__ == "__main__":
    main()
