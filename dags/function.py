from googleapiclient.discovery import build


def trigger_df_job(cloud_event, environment):
    service = build("dataflow", "v1b3")
    project = "automated-stockmarket-pipeline"

    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    template_body = {
        "jobName": "automated_pipeline_dataflow",
        "parameters": {
            "javascriptTextTransformGcsPath": "gs://stockmarket-data/stoks_dataflow_metadata/udf.js",
            "JSONPath":"gs://stockmarket-data/stoks_dataflow_metadata/bq_schema.json"
            ,"javascriptTextTransformFunctionName": "transform",
            'outputTable': "automated-stockmarket-pipeline:automated_stockmarket_pipeline.tata_motors",
            "inputFilePattern": "gs://stockmarket-data/raw/TATAMOTORS.NS/TATAMOTORS.NS_20251003_144610.csv"
            , "bigQueryLoadingTemporaryDirectory":"gs://stockmarket-data/raw/TATAMOTORS.NS/tatamotors_temp/"
        }
    }

    request = service.projects().templates().launch(projectId = project, gcsPath = template_path, body = template_body )
    response = request.execute()
    print(response)