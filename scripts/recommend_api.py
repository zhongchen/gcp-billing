#!/usr/bin/python

# Copyright 2019 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.

import argparse
import json
from pathlib import Path
import threading
from datetime import datetime

from google.oauth2 import service_account
from apiclient.discovery import build_from_document
from googleapiclient import discovery
from google.cloud import storage
from google.cloud import bigquery

#SERVICE_ACCOUNT_FILE = str(Path.home()) + '/achintapatla-project-df6e964e0b56.json'
SERVICE_ACCOUNT_FILE = 'achintapatla-project-df6e964e0b56.json'
RECOMMENDER_DISCOVERY_DOC = 'recommender_discovery.json'
SCOPE = 'https://www.googleapis.com/auth/cloud-platform'

"""
RECOMMENDER = 'google.compute.instance.MachineTypeRecommender'
bq_table_id = 'MachineTypeRecommender'
gcs_bucket = 'achintapatla-co-machine-type-recommender'
"""

RECOMMENDER = 'google.compute.instance.IdleResourceRecommender'
bq_table_id = 'IdleResourceRecommender'
gcs_bucket = 'achintapatla-co-idle-resource-recommender'

RECOMMENDATION_FILE_PREFIX = 'rec_output-'

bq_dataset_id = 'cost_optimization'

gcs_project = 'achintapatla-project'
date_time = datetime.now()
bucket_prefix = date_time.strftime("%Y-%m-%d")


def get_credentials():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=[SCOPE])
    return credentials


def get_zones_list(credentials, project):
    service = discovery.build('compute', 'v1', credentials=credentials)
    zones = []
    response = service.zones().list(project=project).execute()
    for r in response['items']:
        zones.append(r['name'])
    return zones


def get_projects_list(credentials):
    service = discovery.build('cloudresourcemanager', 'v1', credentials=credentials)
    projects = []
    request = service.projects().list()
    response = request.execute()
    for p in response['projects']:
        if (p['lifecycleState'] == 'ACTIVE'):
            projects.append(p['projectId'])
            print("projectId {}".format(p['projectId']))
    return projects


def get_recommendation_service(credentials):
    with open(RECOMMENDER_DISCOVERY_DOC, 'r') as f:
        doc = f.read()
        return build_from_document(doc, credentials=credentials)

def get_recommendation_service_ga(credentials):
    service = discovery.build('recommender', 'v1', credentials=credentials)
    return service

def upload_to_gcs(project, bucket, target, data):
    print("BEFORE upload_to_gcs bucket {0} file {1}", bucket, target)
    gcs = storage.Client(gcs_project)
    print("AFTER gcs")

    try:
        #output_bucket = gcs.get_bucket(bucket)
        output_bucket = gcs.bucket(bucket)
        print("AFTER output_bucket")
        blob=output_bucket.blob(target)
        print("AFTER blob")
        ret_val = blob.upload_from_string(data)
        print("AFTER upload_to_gcs bucket {0} file {1} ret {2}", bucket, target, ret_val)
    #except google.cloud.exceptions.NotFound:
    #    print("Sorry, that bucket does not exist! {0}", bucket)
    except Exception as ex:
        template = "upload_to_gcs:: exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print (message)


def bulk_load_bq(dataset, table, gcs_uri):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

    load_job = client.load_table_from_uri(
        gcs_uri,
        dataset_ref.table(table),
        job_config=job_config,
    )
    print("Starting BigQuery bulk load job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("BigQuery Job finished.")


def collect_recommendations_data(credentials, service, p):
    zones = get_zones_list(credentials, p)
    for z in zones:
        parent = str.format('projects/{0}/locations/{1}/recommenders/{2}', p,
                            z, RECOMMENDER)

        request = service.projects().locations().recommenders().recommendations().list(
            parent=parent)

        try:
            response = request.execute()
            print("===========================================================")
            print("collect_recommendations_data {} : {}".format(p,z))
            if response:
                json_string =  "\n".join(json.dumps(x) for x in response['recommendations'])

                #print(json_string[])
                #print("Response json {}".format(json_string))
                gcs_json_file = bucket_prefix + '/' + \
                                RECOMMENDATION_FILE_PREFIX + \
                                p + "-" + z + ".json"
                print("gcs_json_file {}".format(gcs_json_file))
                upload_to_gcs(p, gcs_bucket, gcs_json_file, json_string)
            print("===========================================================")

        #except:
        #    print("===========================================================")
        #    print("API Error in Project {}, Zone {}".format(p, z))
        except Exception as ex:
            template = "collect_recommendations_data:: exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print (message)

credentials = get_credentials()
service = get_recommendation_service_ga(credentials)
projects = get_projects_list(credentials)

#projects =['psosapproj']
print("Execute")

"""
api_threads = []
for p in projects:
    api_thread = threading.Thread(target=collect_recommendations_data,
                                  args=(credentials, service, p), name=p)
    api_threads.append(api_thread)
    api_thread.start()

# Wait for all threads to complete
for i in api_threads:
    i.join()
"""
for p in projects:
    try:
        collect_recommendations_data(credentials, service, p)
    except Exception as ex:
        template = "for loop:: exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print (message)

gcs_uri = "gs://" + gcs_bucket + "/" + bucket_prefix + "/*.json"
bulk_load_bq(bq_dataset_id, bq_table_id, gcs_uri)