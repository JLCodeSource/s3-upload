#!/bin/bash

# Source env data
if [ ! -f /workspaces/s3-upload/.devcontainer/.env ]
then
    echo Add .devcontainer/.env to export aws vars
    echo export AWS_ACCESS_KEY_ID=XXX
    echo export AWS_SECRET_ACCESS_KEY=YYY
    echo export AWS_DEFAULT_REGION=ZZZ
else
    source /workspaces/s3-upload/.devcontainer/.env
fi