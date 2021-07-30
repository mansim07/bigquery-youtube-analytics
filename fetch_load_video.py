# -*- coding: utf-8 -*-

# Sample Python code for youtube.videos.list
# See instructions for running these code samples locally:
# https://developers.google.com/explorer-help/guides/code_samples#python

import os
import csv

import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors

from google.cloud import bigquery

scopes = ["https://www.googleapis.com/auth/youtube.readonly"]


def main():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "client_key.json"
    part_val = "contentDetails,snippet"
    id_val = "aew6htG_KAY,Nq1OOEvA6lw"

    file_path = 'video_meta.csv'
    table_id = "mnzis-project-01.bq_demo.video_metadata3"
    schema = [bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
              bigquery.SchemaField("video_title", "STRING", mode="REQUIRED")]
    header = ['video_id', 'video_title']

    # Get credentials and create an API client
    flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
        client_secrets_file, scopes)
    credentials = flow.run_console()
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, credentials=credentials)

    request = youtube.videos().list(
        part=part_val,
        id=id_val
    )
    response = request.execute()

    with open(file_path, mode='w') as video_meta:
        video_writer = csv.writer(
            video_meta, delimiter='|', quotechar='"', quoting=csv.QUOTE_ALL)
        video_writer.writerow(header)

        for i in range(len(response.get('items'))):
            video_writer.writerow([str(response.get('items')[i].get('id')), str(
                response.get('items')[i].get('snippet').get('title'))])
            #print(response.get('items')[i].get('snippet').get('title'))
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    # table_id = "your-project.your_dataset.your_table_name"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True, schema=schema
    )
    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(
            source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


if __name__ == "__main__":
    main()
