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

    # Conect to GCS and pull `manifest.json` into a native dict
    # storage_client = storage.Client()
    # bucket = storage_client.get_bucket('cse-jeryeo-test-bucket')
    # blob = bucket.blob('dbt/manifest.json')
    # manifest = json.loads(blob.download_as_text(client=None))
    # spark.conf.set("spark.sql.execution.arrow.enabled", "false")

    fake_data = {
        "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
        "dbt_version": "1.3.3",
        "generated_at": "2023-03-30T02:01:24.594394Z",
        "invocation_id": "3958cbaf-dc93-4a3f-9070-5399",
        "env__DBT_CLOUD_PROJECT_ID": "100806",
        "env__DBT_CLOUD_RUN_ID": "135474427",
        "env__DBT_CLOUD_JOB_ID": "161374",
        "env__DBT_CLOUD_RUN_REASON": "scheduled",
        "env__DBT_CLOUD_RUN_REASON_CATEGORY": "scheduled",
        "project_id": "77acd26fe2b733f4b03714f3dce35424",
        "user_id": "4d7386d2-a078-4627-be50-91ec0c042f5b",
        "send_anonymous_usage_stats": True,
        "adapter_type": "snowflake",
        "node": "model.rapid_onboarding_exemplar.sample",
        "compiled": True,
        "resource_type": "model",
        "depends_on__macros": ['macro.rapid_onboarding_exemplar.money'],
        "depends_on__nodes": ['snapshot.rapid_onboarding_exemplar.snapshot_stg_payments'],
        "config__enabled": True,
        "config__alias": "relationships_fct_orders_0c6c6d9e6f30dfb9b653557ebf38e47c",
        "config__schema": "dbt_test__audit",
        "config__database": "analytics",
        "config__tags": ['tag', 'finance'],
        "config__meta": {"some_key": "some_value"},
        "config__materialized": "view",
        "config__incremental_strategy": "delete+insert",
        "config__persist_docs":{"some_key": "some_value"},
        "config__quoting":{"some_key": "some_value"},
        "config__column_types":{"some_key": "some_value"},
        "config__full_refresh": "not_configured",
        "config__unique_key": "order_item_id",
        "config__on_schema_change": "ignore",
        "config__grants": "order_item_id",
        "config__packages": {"some_key": "some_value"},
        "config__docs": {'show': True, 'node_color': 'blue'},
        "config__post-hook": ['select 1'],
        "config__pre-hook": ['select 1'],
        "database": "analytics",
        "schema": "analytics",
        "fqn": ['rapid_onboarding_exemplar', 'snapshot_stg_payments', 'snapshot_stg_payments'],
        "unique_id": "3958cbaf-dc93-4a3f-9070-53993caebcd0__model.rapid_onboarding_exemplar.stg_tpch__customers",
        "raw_code": "select * from { ref('customers') }",
        "language": "sql",
        "package_name": "rapid_onboarding_exemplar",
        "root_path": "/tmp/jobs/135474427/target",
        "path": "staging/stripe/stg_stripe__payments.sql",
        "original_file_path": "models/staging/stripe/stg_stripe__payments.sql",
        "name": "stg_stripe__payments",
        "alias": "stg_stripe__payments",	
        "checksum": {'name': 'sha256', 'checksum': '66ebcab614927b4d0d9b034e9a06f197ce2005a510c3ed24ffe527bfef2de6ce'},
        "tags": ['tag', 'finance'],
        "refs": [['int_order_items_joined'], ['int_part_suppliers_joined']],
        "sources": [['tpch', 'partsupp']],
        "metrics": [['total_revenue']],
        "description": "Aggregated orders data by customer",
        "columns": {"some_key": "some_value"},
        "meta": {"some_key": "some_value"},
        "docs": {'show': True, 'node_color': 'None'},
        "patch_path": "rapid_onboarding_exemplar://models/staging/tpch/_tpch__models.yml",
        "compiled_path": "target/compiled/rapid_onboarding_exemplar/models/staging/tpch/stg_tpch__customers.sql",
        "build_path": "rapid_onboarding_exemplar://models/staging/tpch/_tpch__models.yml",
        "deferred": False,
        "unrendered_config": {'materialized': 'table', 'tags': ['tag']},
        "created_at": "1680141624.200500",
        "compiled_code": "select * from database.schema.table",
        "extra_ctes_injected": True,
        "extra_ctes": ['something'],
        "relation_name": "analytics.analytics.stg_tpch__suppliers",
        "config__transient": False,
        "config__strategy": "check",
        "config__target_schema": "snapshots",
        "config__check_cols": ['gross_item_sales_amount', 'net_item_sales_amount'],
        "config__target_database": "analytics",
        "config__updated_at": "_BATCHED_AT",
        "config__severity": "ERROR",
        "config__store_failures": "empty",
        "config__where": "empty",
        "config__limit": "empty",
        "config__fail_calc": "count(*)",
        "config__warn_if": "!= 0",
        "config__error_if": "!= 0",
        "test_metadata": {"some_key": "some_value"},
        "column_name": "customer_id",
        "file_key_name": "models.stg_tpch__customers",
    }


    gcs_file_system = gcsfs.GCSFileSystem(project="cse-sandbox-319708")
    gcs_json_path = "gs://cse-jeryeo-test-bucket/dbt/manifest.json"
    list_of_dicts = []
    with gcs_file_system.open(gcs_json_path) as f:
        manifest = json.load(f)
        for manifest_node, manifest_dict in manifest['nodes'].items():
            record_dict = {}
            for k,v in manifest['metadata'].items():
                if k == 'env':
                    for sub_k, sub_v in v.items():
                        record_dict[f'{k}__{sub_k}'] = str(sub_v)
                else:
                    record_dict[k] = v
            record_dict['node'] = manifest_node
            for k,v in manifest_dict.items():
                # custom unpacking logic here:
                if k in ('depends_on', 'config'):
                    for sub_k, sub_v in v.items():
                        record_dict[f'{k}__{sub_k}'] = str(sub_v)
                else:
                    record_dict[k] = str(v)
            record_dict['unique_id'] = str(record_dict['invocation_id']+'__'+record_dict['node'])
            list_of_dicts.append(record_dict)

    # nodes_df = pd.DataFrame(list_of_dicts)
    # nodes_df_filled = nodes_df.replace({np.nan: None})
    
    fake_df = pd.DataFrame(list_of_dicts)

    return fake_df
    
