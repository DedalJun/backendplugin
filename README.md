Run commands from directory where located project 


For building use command:
gcloud builds submit --tag gcr.io/PROJECT-ID/type_your_own_name_to_plugin

For deploying use command:
gcloud run deploy --image gcr.io/PROJECT-ID/name_of_plugin --platform managed
