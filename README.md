# EMR Serverless Example
Terraform EMR Serverless creation and a sample Python Spark job runner.

## Terraform
See versions.tf for TF version requirements.
See the "variables.tf" file for variable description. Application name and size can be configured.

```terraform init && terraform apply```

## Submitting a job
See run_job.py file for example parameters.
**Note:** This repo does not include any job code. A standard Scala uberjar (or Python equivalent) is required.

```bash
python -m venv .venv
source .venv/bin/activate
pip install boto3
python run_job.py
```

