#!/bin/sh

FUNCTION="logPkbAlerts"
PROJECT="smu-benchmarking"
BUCKET="gs://smu_pkb_run_logs"
ENV_VARS_FILE="env_vars.yaml"

# set the gcloud project
gcloud config set project ${PROJECT}

gcloud functions deploy logPkbAlerts \
    --runtime="python37" \
    --trigger-resource="${BUCKET}"  \
    --trigger-event="google.storage.object.finalize" \
    --env-vars-file="${ENV_VARS_FILE}" \
    #--trigger-http  \
    #--trigger-bucket=${BUCKET} \


# gcloud functions deploy logPkbAlerts --env-vars-file="env_vars.yaml" --trigger-resource="gs://smu_pkb_run_logs" --trigger-event="google.storage.object.finalize"
