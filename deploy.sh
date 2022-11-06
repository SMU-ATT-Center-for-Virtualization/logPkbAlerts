#!/bin/sh

FUNCTION="logPkbAlerts"
PROJECT="smu-benchmarking"
PUB_SUB_TOPIC="log_pkb_alerts"
ENV_VARS_FILE="env_vars.yaml"

# set the gcloud project
gcloud config set project ${PROJECT}

gcloud functions deploy logPkbAlerts \
    --runtime="python39" \
    --trigger-topic="${PUB_SUB_TOPIC}" \
    --env-vars-file="${ENV_VARS_FILE}"


# gcloud functions deploy logPkbAlerts --env-vars-file="env_vars.yaml" --trigger-resource="gs://smu_pkb_run_logs" --trigger-event="google.storage.object.finalize"
