# Dataflow job to unzip locked files
This dataflow job read the bulk locked zipped files and unlock them. Password has to be configured in Secret Manager 
and you need pass on the secret key name to this dataflow parameter so that it can read the latest version of key and use that to unlock the file.

## Compilation
mvn clean package -Pdataflow-runner

## Local environment
* Set google credentials: ```export GOOGLE_APPLICATION_CREDENTIALS=<credetials_file.json>```
* Run pipeline locally: ```mvn compile exec:java -Dexec.mainClass=com.mwp.decompress.UnlockZipPipeline```

## Create Dataflow Flex Template
* Set GCR Image Path: ```export TEMPLATE_IMAGE="gcr.io/project-id/data-analytics-foundation/unzip-files:latest"```
* Set Dataflow Template Path: ```export TEMPLATE_PATH="gs://bucket_name/bulk-unzip-gcs-files"```
* Build Dataflow flex template: ```gcloud dataflow flex-template build $TEMPLATE_PATH_UNZIP --image-gcr-path "$TEMPLATE_IMAGE_UNZIP" --sdk-language "JAVA" --flex-template-base-image JAVA11 --metadata-file "Bulk_Unzip_Protected_GCS_Files_metadata.json" --jar "target/UnlockZipFiles-bundled-v1.0.jar" --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.mwp.decompress.UnlockZipPipeline"```
* Deploy Dataflow job from just created flex template: ```gcloud dataflow flex-template run "unzip-files-`date +%Y%m%d-%H%M%S`"     --template-file-gcs-location "$TEMPLATE_PATH"     --service-account-email=mw-data-bq-data-catalog@project-id.iam.gserviceaccount.com     --parameters inputFilePattern="gs://bucket_name/zipfiles*" --parameters outputDirectory="gs://bucket_name/unlockzipfiles" --parameters outputFailureFile="gs://bucket_name/failed.csv" --parameters streamBufferSize="4096" --parameters exceptionLocation="gs://bucket_name/zipexception" --region "us-east1"```
