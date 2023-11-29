# s3-upload

## Install
1. Install Poetry
curl -sSL https://install.python-poetry.org | python3 -
2. Install deps
poetry install
3. Add aws creds
echo export AWS_ACCESS_KEY_ID=XXX > .env
echo export AWS_SECRET_ACCESS_KEY=YYY >> .env
echo export AWS_DEFAULT_REGION=ZZZ >> .env

## Run
Usage: python s3.py source_dir status_file max_size \[log_file\]