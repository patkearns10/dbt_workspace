import gcsfs
import json

from google.cloud import storage
import pandas as pd
import numpy as np

def model(dbt, session):

    # dbt model configs. Ideally submission_method should be `cluster`
    dbt.config(
        materialized = "table",
        submission_method = "cluster",
        dataproc_cluster_name="cse-jeryeo-cluster",
        packages = ["numpy", "pandas"]
        )

    gcs_file_system = gcsfs.GCSFileSystem(project="cse-sandbox-319708")
    gcs_json_path = "gs://cse-jeryeo-test-bucket/dbt/manifest.json"
    
    # Could also do it this way:
    # Conect to GCS and pull `manifest.json` into a native dict
    # storage_client = storage.Client()
    # bucket = storage_client.get_bucket('cse-jeryeo-test-bucket')
    # blob = bucket.blob('dbt/manifest.json')
    # manifest = json.loads(blob.download_as_text(client=None))
    
    list_of_dicts = []
    with gcs_file_system.open(gcs_json_path) as f:
        manifest = json.load(f)
        for manifest_node, manifest_dict in manifest['nodes'].items():
            record_dict = {}
            # get the metadata for which manifest.json 
            for k,v in manifest['metadata'].items():
                if k == 'env':
                    for sub_k, sub_v in v.items():
                        record_dict[f'{k}__{sub_k}'] = str(sub_v)
                else:
                    record_dict[k] = str(v)
            record_dict['node'] = manifest_node
            # for each node, get values
            for k,v in manifest_dict.items():
                # custom unpacking logic here for nested structures:
                if k in ('depends_on', 'config'):
                    for sub_k, sub_v in v.items():
                        record_dict[f'{k}__{sub_k}'] = str(sub_v)
                else:
                    record_dict[k] = str(v)
            record_dict['unique_id'] = str(record_dict['invocation_id']+'__'+record_dict['node'])
            list_of_dicts.append(record_dict)
    
    df = pd.DataFrame(list_of_dicts)

    return df
