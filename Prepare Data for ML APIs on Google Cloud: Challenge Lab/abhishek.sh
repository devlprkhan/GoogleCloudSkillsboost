#!/bin/bash
# Define color variables
BLACK=`tput setaf 0`
RED=`tput setaf 1`
GREEN=`tput setaf 2`
YELLOW=`tput setaf 3`
BLUE=`tput setaf 4`
MAGENTA=`tput setaf 5`
CYAN=`tput setaf 6`
WHITE=`tput setaf 7`

BOLD=`tput bold`
RESET=`tput sgr0`

#----------------------------------------------------start--------------------------------------------------#

echo "${CYAN}${BOLD}"
echo "   ____ _               _   ____       _     _               "
echo "  / ___| | ___  _ __ __| | | __ ) _ __(_) __| | _____      __"
echo " | |   | |/ _ \| '__/ _\` | |  _ \| '__| |/ _\` |/ _ \ \ /\ / /"
echo " | |___| | (_) | | | (_| | | |_) | |  | | (_| | (_) \ V  V / "
echo "  \____|_|\___/|_|  \__,_| |____/|_|  |_|\__,_|\___/ \_/\_/  "
echo "${RESET}"
echo "${YELLOW}${BOLD}Subscribe to Dr. Abhishek: https://www.youtube.com/@drabhishek.5460/videos${RESET}"
echo

echo "${GREEN}${BOLD}Starting Execution...${RESET}"
echo

# Function to get input from the user
get_input() {
    local prompt="$1"
    local var_name="$2"
    echo -n -e "${BOLD}${CYAN}${prompt}${RESET} "
    read input
    export "$var_name"="$input"
}

# Gather inputs for the required variables
get_input "Enter the DATASET value (e.g., lab_246):" "DATASET"
get_input "Enter the TABLE value (e.g., customers_309):" "TABLE"
get_input "Enter the BUCKET name (e.g., qwiklabs-gcp-xx-xxxxxxxx-marking):" "BUCKET"
get_input "Enter the BUCKET_URL_1 (e.g., gs://your-bucket/task3-gcs-650.result):" "BUCKET_URL_1"
get_input "Enter the BUCKET_URL_2 (e.g., gs://your-bucket/task4-cnl-541.result):" "BUCKET_URL_2"

echo

# --- Set project and region ---
export PROJECT_ID=$(gcloud config get-value project)
export REGION=$(gcloud compute project-info describe --format="value(commonInstanceMetadata.items[google-compute-default-region])")
export PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="json" | jq -r '.projectNumber')
export USER_EMAIL=$(gcloud config get-value account)

echo "${BLUE}Project: $PROJECT_ID, Region: $REGION, User: $USER_EMAIL${RESET}"

# --- Task 1: Dataflow ---
echo "${GREEN}${BOLD}Task 1: Creating BigQuery dataset and table...${RESET}"
bq mk --location=$REGION $DATASET 2>/dev/null || true

# Create schema file
cat > lab.schema <<EOF
[
  {"type":"STRING","name":"guid"},
  {"type":"BOOLEAN","name":"isActive"},
  {"type":"STRING","name":"firstname"},
  {"type":"STRING","name":"surname"},
  {"type":"STRING","name":"company"},
  {"type":"STRING","name":"email"},
  {"type":"STRING","name":"phone"},
  {"type":"STRING","name":"address"},
  {"type":"STRING","name":"about"},
  {"type":"TIMESTAMP","name":"registered"},
  {"type":"FLOAT","name":"latitude"},
  {"type":"FLOAT","name":"longitude"}
]
EOF

bq mk --table --location=$REGION $DATASET.$TABLE lab.schema 2>/dev/null || true

echo "${BLUE}Creating Cloud Storage bucket...${RESET}"
gsutil mb gs://$BUCKET 2>/dev/null || true

echo "${MAGENTA}Running Dataflow job...${RESET}"
gcloud dataflow jobs run awesome-jobs \
    --gcs-location gs://dataflow-templates-$REGION/latest/GCS_Text_to_BigQuery \
    --region $REGION \
    --worker-machine-type e2-standard-2 \
    --staging-location gs://$BUCKET/temp \
    --parameters \
inputFilePattern=gs://cloud-training/gsp323/lab.csv,\
JSONPath=gs://cloud-training/gsp323/lab.schema,\
outputTable=$PROJECT_ID:$DATASET.$TABLE,\
bigQueryLoadingTemporaryDirectory=gs://$BUCKET/bigquery_temp,\
javascriptTextTransformGcsPath=gs://cloud-training/gsp323/lab.js,\
javascriptTextTransformFunctionName=transform

echo "${YELLOW}Waiting 30 seconds for Dataflow job to start...${RESET}"
sleep 30

# --- Task 2: Dataproc ---
echo "${GREEN}${BOLD}Task 2: Setting up Dataproc cluster...${RESET}"

# Grant IAM roles for Dataproc (just in case)
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member "serviceAccount:$PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
    --role "roles/storage.admin" 2>/dev/null || true

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member=user:$USER_EMAIL \
    --role=roles/dataproc.editor 2>/dev/null || true

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member=user:$USER_EMAIL \
    --role=roles/storage.objectViewer 2>/dev/null || true

# Enable private Google access (if not already)
gcloud compute networks subnets update default \
    --region $REGION \
    --enable-private-ip-google-access 2>/dev/null || true

echo "${CYAN}Creating Dataproc cluster 'awesome' with n2d-standard-2 machines...${RESET}"
gcloud dataproc clusters create awesome \
    --enable-component-gateway \
    --region $REGION \
    --master-machine-type n2d-standard-2 \
    --master-boot-disk-type pd-standard \
    --master-boot-disk-size 100 \
    --num-workers 2 \
    --worker-machine-type n2d-standard-2 \
    --worker-boot-disk-type pd-standard \
    --worker-boot-disk-size 100 \
    --image-version 2.0-debian12 \
    --project $PROJECT_ID

# Wait for cluster to be ready
echo "${YELLOW}Waiting for cluster to become ready...${RESET}"
sleep 60

# Get VM name and zone for SSH
VM_NAME=$(gcloud compute instances list --project="$PROJECT_ID" --format=json | jq -r '.[0].name')
export ZONE=$(gcloud compute instances list --project="$PROJECT_ID" --format="csv[no-heading](zone)" | head -1)

echo "${BLUE}Copying data.txt to HDFS and local on master node...${RESET}"
gcloud compute ssh --zone "$ZONE" "$VM_NAME" --project "$PROJECT_ID" --quiet --command="hdfs dfs -cp gs://cloud-training/gsp323/data.txt /data.txt" 2>/dev/null || true
gcloud compute ssh --zone "$ZONE" "$VM_NAME" --project "$PROJECT_ID" --quiet --command="gsutil cp gs://cloud-training/gsp323/data.txt /data.txt" 2>/dev/null || true

echo "${MAGENTA}Submitting Spark PageRank job...${RESET}"
gcloud dataproc jobs submit spark \
    --cluster=awesome \
    --region=$REGION \
    --class=org.apache.spark.examples.SparkPageRank \
    --jars=file:///usr/lib/spark/examples/jars/spark-examples.jar \
    --project=$PROJECT_ID \
    -- /data.txt

echo "${YELLOW}Spark job submitted. Waiting 30 seconds...${RESET}"
sleep 30

# --- Task 3: Speech-to-Text ---
echo "${GREEN}${BOLD}Task 3: Speech-to-Text API...${RESET}"

# Create API key if not exists (or use existing)
gcloud services enable apikeys.googleapis.com 2>/dev/null || true
KEY_NAME=$(gcloud alpha services api-keys list --format="value(name)" --filter "displayName=awesome" 2>/dev/null)
if [ -z "$KEY_NAME" ]; then
    gcloud alpha services api-keys create --display-name="awesome" > /dev/null 2>&1
    KEY_NAME=$(gcloud alpha services api-keys list --format="value(name)" --filter "displayName=awesome")
fi
API_KEY=$(gcloud alpha services api-keys get-key-string $KEY_NAME --format="value(keyString)")

# Prepare request JSON
cat > request.json <<EOF
{
  "config": {
    "encoding": "FLAC",
    "languageCode": "en-US"
  },
  "audio": {
    "uri": "gs://cloud-training/gsp323/task3.flac"
  }
}
EOF

echo "${CYAN}Sending speech recognition request...${RESET}"
curl -s -X POST -H "Content-Type: application/json" --data-binary @request.json \
    "https://speech.googleapis.com/v1/speech:recognize?key=${API_KEY}" > result.json

echo "${BLUE}Uploading result to $BUCKET_URL_1...${RESET}"
gsutil cp result.json $BUCKET_URL_1
gsutil setmeta -h "Content-Type:application/json" $BUCKET_URL_1

# --- Task 4: Natural Language ---
echo "${GREEN}${BOLD}Task 4: Cloud Natural Language API...${RESET}"

cat > nl_request.json <<EOF
{
  "document": {
    "type": "PLAIN_TEXT",
    "content": "Old Norse texts portray Odin as one-eyed and long-bearded, frequently wielding a spear named Gungnir and wearing a cloak and a broad hat."
  }
}
EOF

echo "${MAGENTA}Sending entity analysis request...${RESET}"
curl -s -X POST -H "Content-Type: application/json" \
    -H "Authorization: Bearer $(gcloud auth print-access-token)" \
    "https://language.googleapis.com/v1/documents:analyzeEntities" \
    -d @nl_request.json > result2.json

echo "${BLUE}Uploading result to $BUCKET_URL_2...${RESET}"
gsutil cp result2.json $BUCKET_URL_2
gsutil setmeta -h "Content-Type:application/json" $BUCKET_URL_2

echo "${GREEN}${BOLD}"
echo "Lab tasks completed! Please check your progress in the console."
echo "${YELLOW}${BOLD}Subscribe to Dr. Abhishek: https://www.youtube.com/@drabhishek.5460/videos${RESET}"
echo

# Cleanup
rm -f request.json nl_request.json lab.schema result.json result2.json
