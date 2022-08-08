# EMR Serverless Example
Terraform EMR Serverless creation and a sample Python Spark job runner.

## Terraform
See versions.tf for TF version requirements.
See the "variables.tf" file for variable description. Application name and size can be configured.
The *dump_config.sh* shell script will dump the TF output variables to a JSON config used by the Python
job code.

```
terraform init && terraform apply
./dump_config.sh
```

## Submitting a job

Uses a file: config.json to store global application parameters. Can be easily created by dump_config.sh:
```json
{
  "application_id": {
    "value": "<serverless app id>"
  },
  "execution_role_arn": {
    "value": "<role used by application for execution>"
  }
}
```

See run_job.py file for example parameters.
**Note:** This repo does not include any job code. A standard Scala uberjar (or Python equivalent) is required.

```bash
python -m venv .venv
source .venv/bin/activate
pip install boto3
python run_job.py
```

