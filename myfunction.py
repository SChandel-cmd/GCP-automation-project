from googleapiclient.discovery import build


def trigger_df_job(cloud_event,environment):   
 
    service = build('dataflow', 'v1b3')
    project = "dag-airflow-poc"

    template_path = "gs://dataflow-templates-asia-south2/latest/GCS_CSV_to_BigQuery"

    template_body = {
        "jobName": "dataflow-job-poc",  # Provide a unique name for the job
        "parameters": {
        "schemaJSONPath": "gs://dataflow-metadata-poc/bq.json",
        "delimiter": ",",
        "csvFormat": "Default",
        "outputTable": "dag-airflow-poc:cricket.icc_test_batsmen_ranking",
        "inputFilePattern": "gs://bkt-ranking-data-poc/batsmen_rankings.csv",
        "bigQueryLoadingTemporaryDirectory": "gs://dataflow-metadata-poc/temp_bq/",
        "badRecordsOutputTable": "dag-airflow-poc:cricket.bad_records",
        }
    }

    request = service.projects().templates().launch(projectId=project,gcsPath=template_path, body=template_body)
    response = request.execute()
    print(response)

