from google.cloud import storage
import json
from datetime import datetime
from pyspark.sql import types as T
import pandas as pd

def model(dbt, session):

    # dbt model configs. Ideally submission_method should be `cluster`
    dbt.config(materialized = "table")
    dbt.config(full_refresh = True)
    dbt.config(submission_method = "serverless")
    dbt.config(unique_id = 'unique_id')

    # Manifest nodes data types declaration. Update it to include/exclude/edit fields
    node_keys = {
        'resource_type': T.StringType(),
        'depends_on': T.StructType([
            T.StructField('macros', T.ArrayType(T.StringType())),
            T.StructField('nodes', T.ArrayType(T.StringType()))
            ]),
        'config': T.StructType([
            T.StructField('enabled', T.BooleanType()),
            T.StructField('alias', T.StringType()),
            T.StructField('schema', T.StringType()),
            T.StructField('database', T.StringType()),
            T.StructField('tags', T.ArrayType(T.StringType())),
            T.StructField('materialized', T.StringType()),
            T.StructField('incremental_strategy', T.StringType()),
            T.StructField('unique_key', T.StringType()),
            T.StructField('on_schema_change', T.StringType()),
            T.StructField('grants', T.StringType()),
            T.StructField('post-hook', T.ArrayType(T.StringType())),
            T.StructField('pre-hook', T.ArrayType(T.StringType())),
            T.StructField('severity', T.StringType()),
            T.StructField('fail_calc', T.StringType()),
            T.StructField('warn_if', T.StringType()),
            T.StructField('error_if', T.StringType())
        ]),
        'database': T.StringType(),
        'schema': T.StringType(),
        'fqn': T.ArrayType(T.StringType()),
        'unique_id': T.StringType(),
        'language': T.StringType(),
        'package_name': T.StringType(),
        'original_file_path': T.StringType(),
        'name': T.StringType(),
        'alias': T.StringType(),
        'tags': T.ArrayType(T.StringType()),
        'refs': T.ArrayType(T.ArrayType(T.StringType())),
        'sources': T.ArrayType(T.ArrayType(T.StringType())),
        'metrics': T.ArrayType(T.ArrayType(T.StringType())),
        'description': T.StringType(),
        'docs': T.StructType([
            T.StructField('show', T.BooleanType()),
            T.StructField('node_color', T.StringType())
        ]),
        'patch_path': T.StringType(),
        'compiled_path': T.StringType(),
        'test_metadata': T.StructType([
            T.StructField('name',T.StringType()),
            T.StructField('kwargs',T.StructType([
                T.StructField('column_name',T.StringType()),
                T.StructField('model',T.StringType())
            ])),
            T.StructField('namespace',T.StringType())
        ]),
        'build_path': T.StringType(),
        'deferred': T.BooleanType(),
        'column_name': T.StringType(),
        'file_key_name': T.StringType()

    }

    # Conect to GCS and pull `manifest.json` into a native dict
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('cse-jeryeo-test-bucket')
    blob = bucket.blob('dbt/manifest.json')
    manifest = json.loads(blob.download_as_string(client=None))

    # Parse nodes keys to a list, normalize columns in a list and convert timestamp object
    nodes = list(manifest["nodes"].values())
    # models = []
    # for node in nodes:
    #     node['columns'] = list(node['columns'].values())
    #     node['created_at'] = datetime.fromtimestamp(node['created_at'])
    #     models.append(node)

    # Selects only the json keys specified in the Data Type declaration `keys`
    # models = [{k:node[k] for k in node_keys.keys()} for node in models]

    df = pd.DataFrame(nodes)

    return df