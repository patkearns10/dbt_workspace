import csv
from datetime import datetime
import json

import pandas as pd
import streamlit as st
from streamlit.runtime.uploaded_file_manager import UploadedFile

now = datetime.now().strftime('%Y.%m.%d.%H.%M.%S')
multiline_comment = "'''"

# some helper macros
def write_relationships(file_name, primary_keys, foreign_keys, metrics, measures):
    # create pk --> fk relationships
    file_name.append('\n')
    for primary_key in list(set(primary_keys)):
        for foreign_key in list(set(foreign_keys)):
            # Ref: sem_date.date_key - sem_sale_item.date_key
            if primary_key.split('.')[1] == foreign_key.split('.')[1]:
                if primary_key.split('.')[1] == 'date_key':
                    file_name.append(f'Ref: {primary_key} - {foreign_key}'+'\n')
                else:
                    file_name.append(f'Ref: {primary_key} < {foreign_key}'+'\n')
    for metric in list(set(metrics)):
        for m in list(set(metrics)):
            if metric.split('.')[1] == m.split('.')[0] and metric != m:
                file_name.append(f'Ref: {metric} - {m}'+'\n')
    for metric in list(set(metrics)):
        for measure in list(set(measures)):
            if metric.split('.')[1] == measure.split('.')[1] and metric != measure:
                file_name.append(f'Ref: {metric} - {measure}'+'\n')

def recursive_metric_finder(manifest, metrics, file_name, metrics_consumed):
    # metrics can reference other metrics, so for those types, recursively find the metrics
    for r_metric in metrics:
        for manifest_metric in manifest['metrics']:
            if manifest_metric["name"] == r_metric.split('.')[1] and manifest_metric["name"] not in metrics_consumed:
                file_name.append(f'Table {manifest_metric["name"]}'+'\n')
                file_name.append('{'+'\n')
                file_name.append(f'Note: {multiline_comment}{manifest_metric["description"]} -- type: {manifest_metric["type"]}{multiline_comment}'+'\n')
                if manifest_metric["type"] in ('simple', 'cumulative', 'conversion') and manifest_metric["name"] not in metrics_consumed:
                    # these metrics only depend on measures
                    for mm in manifest_metric["type_params"]["input_measures"]:
                        metrics.append(f'{manifest_metric["name"]}.{mm["name"]}')
                        file_name.append(f'{mm["name"]} measure'+'\n')
                    file_name.append('}'+'\n')
                    metrics_consumed.append(manifest_metric["name"])
                    break
                elif manifest_metric["type"] == 'derived' and manifest_metric["name"] not in metrics_consumed:
                    # depends on one or many metrics
                    for mmet in manifest_metric["type_params"]["metrics"]:
                        metrics.append(f'{manifest_metric["name"]}.{mmet["name"]}')
                        file_name.append(f'{mmet["name"]} metric'+'\n')
                        metrics_consumed.append(manifest_metric["name"])
                    file_name.append('}'+'\n')
                elif manifest_metric["type"] == 'ratio' and manifest_metric["name"] not in metrics_consumed:
                    # numerator metric
                    metrics.append(f'{manifest_metric["name"]}.{manifest_metric["type_params"]["numerator"]["name"]}')
                    file_name.append(f'{manifest_metric["type_params"]["numerator"]["name"]} measure_numerator'+'\n')
                    # denominator metric
                    metrics.append(f'{manifest_metric["name"]}.{manifest_metric["type_params"]["denominator"]["name"]}')
                    file_name.append(f'{manifest_metric["type_params"]["denominator"]["name"]} measure_denominator'+'\n')
                    file_name.append('}'+'\n')
                    metrics_consumed.append(manifest_metric["type_params"]["numerator"]["name"])
                    metrics_consumed.append(manifest_metric["type_params"]["denominator"]["name"])
                else:
                    print('Found something unexpected (type not in simple, ratio, derived, conversion)')

def export_metric_dbdiagram_file(manifest, choose_metrics):
    dbdiagram_metric_file = []
    primary_keys = []
    foreign_keys = []
    metrics = []
    measures = []
    metrics_consumed = []
    # get metric - measure matches
    for arg_metric in choose_metrics:
        for metric in manifest['metrics']:
            if metric["name"] == arg_metric and metric["name"] not in metrics_consumed:       
                dbdiagram_metric_file.append(f'Table {metric["name"]}'+'\n')
                dbdiagram_metric_file.append('{'+'\n')
                dbdiagram_metric_file.append(f'Note: {multiline_comment}{metric["description"]} -- type: {metric["type"]}{multiline_comment}'+'\n')
                if metric["type"] in ('simple', 'cumulative', 'conversion') and metric["name"] not in metrics_consumed:
                    for m in metric["type_params"]["input_measures"]:
                        metrics.append(f'{metric["name"]}.{m["name"]}')
                        dbdiagram_metric_file.append(f'{m["name"]} measure'+'\n')
                    dbdiagram_metric_file.append('}'+'\n')
                    metrics_consumed.append(metric["name"])

                elif metric["type"] == 'derived' and metric["name"] not in metrics_consumed:
                    for met in metric["type_params"]["metrics"]:
                        metrics.append(f'{metric["name"]}.{met["name"]}')
                        dbdiagram_metric_file.append(f'{met["name"]} metric'+'\n')
                    dbdiagram_metric_file.append('}'+'\n')
                    metrics_consumed.append(metric["name"])
                    recursive_metric_finder(manifest, metrics, dbdiagram_metric_file, metrics_consumed)

                elif metric["type"] == 'ratio' and metric["name"] not in metrics_consumed:
                    # numerator
                    metrics.append(f'{metric["name"]}.{metric["type_params"]["numerator"]["name"]}')
                    dbdiagram_metric_file.append(f'{metric["type_params"]["numerator"]["name"]} measure_numerator'+'\n')
                    # denominator
                    metrics.append(f'{metric["name"]}.{metric["type_params"]["denominator"]["name"]}')
                    dbdiagram_metric_file.append(f'{metric["type_params"]["denominator"]["name"]} measure_denominator'+'\n')
                    dbdiagram_metric_file.append('}'+'\n')
                    metrics_consumed.append(metric["name"])
                    recursive_metric_finder(manifest, metrics, dbdiagram_metric_file, metrics_consumed)
                else:
                    print('Metrics traversed)')

    # get measures to semantic model matches
    tables_built = []
    for semantic_model in manifest['semantic_models']:
        for measure in semantic_model["measures"]:
            for metric in metrics:
                if metric.split('.')[1] == measure["name"] and semantic_model["name"] not in tables_built:
                    dbdiagram_metric_file.append(f'Table {semantic_model["name"]}'+'\n')
                    dbdiagram_metric_file.append('{'+'\n')
                    dbdiagram_metric_file.append(f'Note: {multiline_comment}{semantic_model["node_relation"]["alias"]} -- {semantic_model["description"]}{multiline_comment}'+'\n')
                    for entity in semantic_model["entities"]:
                        if entity["type"] == 'primary':
                            primary_keys.append(f'{semantic_model["name"]}.{entity["name"]}')
                            dbdiagram_metric_file.append(f'{entity["name"]} primary [primary key]'+'\n')
                        else:
                            foreign_keys.append(f'{semantic_model["name"]}.{entity["name"]}')
                            dbdiagram_metric_file.append(f'{entity["name"]} {entity["type"]}'+'\n')
                    for dimension in semantic_model["dimensions"]:
                        dbdiagram_metric_file.append(f'{dimension["name"]} dimension'+'\n')
                    for measure in semantic_model["measures"]:
                        measures.append(f'{semantic_model["name"]}.{measure["name"]}')
                        dbdiagram_metric_file.append(f'{measure["name"]} measure'+'\n')
                    dbdiagram_metric_file.append('}'+'\n')
                    tables_built.append(semantic_model["name"])
                    
    # get semantic model foreign key matches 
    semantic_models_include = []
    foreign_key_pks = [foreign_key.split('.')[1] for foreign_key in foreign_keys]
    for semantic_model in manifest['semantic_models']:
        for entity in semantic_model["entities"]:
            if entity["type"] == 'primary' and entity["name"] in foreign_key_pks:
                semantic_models_include.append(semantic_model["name"])

    for semantic_model in manifest['semantic_models']:
        if semantic_model["name"] in semantic_models_include and semantic_model["name"] not in tables_built:
            dbdiagram_metric_file.append(f'Table {semantic_model["name"]}'+'\n')
            dbdiagram_metric_file.append('{'+'\n')
            dbdiagram_metric_file.append(f'Note: {multiline_comment}{semantic_model["node_relation"]["alias"]} -- {semantic_model["description"]}{multiline_comment}'+'\n')
            for entity in semantic_model["entities"]:
                if entity["type"] == 'primary':
                    primary_keys.append(f'{semantic_model["name"]}.{entity["name"]}')
                    dbdiagram_metric_file.append(f'{entity["name"]} primary [primary key]'+'\n')
                else:
                    foreign_keys.append(f'{semantic_model["name"]}.{entity["name"]}')
                    dbdiagram_metric_file.append(f'{entity["name"]} {entity["type"]}'+'\n')
            for dimension in semantic_model["dimensions"]:
                dbdiagram_metric_file.append(f'{dimension["name"]} dimension'+'\n')
            for measure in semantic_model["measures"]:
                measures.append(f'{semantic_model["name"]}.{measure["name"]}')
                dbdiagram_metric_file.append(f'{measure["name"]} measure'+'\n')
            dbdiagram_metric_file.append('}'+'\n')
            tables_built.append(semantic_model["name"])

    # create pk --> fk relationships
    write_relationships(dbdiagram_metric_file, primary_keys, foreign_keys, metrics, measures)
    txt_file = ''.join(dbdiagram_metric_file)
    st.download_button(label='click here to download - dbdiagram metric file', data=txt_file, file_name=f'dbdiagram_metric_{now}.txt')

    print('Succesfully created metric dbdiagram.io file!')


def export_dbdiagram_file(manifest):
    dbdiagram_file = []
    primary_keys = []
    foreign_keys = []
    metrics = []
    measures = []
    # output table format for dbdiagram.io
    for semantic_model in manifest['semantic_models']:
        dbdiagram_file.append(f'Table {semantic_model["name"]}'+'\n')
        dbdiagram_file.append('{'+'\n')
        dbdiagram_file.append(f'Note: {multiline_comment}{semantic_model["node_relation"]["alias"]} -- {semantic_model["description"]}{multiline_comment}'+'\n')
        for entity in semantic_model["entities"]:
            if entity["type"] == 'primary':
                primary_keys.append(f'{semantic_model["name"]}.{entity["name"]}')
                dbdiagram_file.append(f'{entity["name"]} primary [primary key]'+'\n')
            else:
                foreign_keys.append(f'{semantic_model["name"]}.{entity["name"]}')
                dbdiagram_file.append(f'{entity["name"]} {entity["type"]}'+'\n')
        for dimension in semantic_model["dimensions"]:
            dbdiagram_file.append(f'{dimension["name"]} dimension'+'\n')
        for measure in semantic_model["measures"]:
            measures.append(f'{semantic_model["name"]}.{measure["name"]}')
            dbdiagram_file.append(f'{measure["name"]} measure'+'\n')
        dbdiagram_file.append('}'+'\n')

    # create metrics tables
    dbdiagram_file.append('\n')
    for metric in manifest['metrics']:
        dbdiagram_file.append(f'Table {metric["name"]}'+'\n')
        dbdiagram_file.append('{'+'\n')
        dbdiagram_file.append(f'Note: {multiline_comment}{metric["description"]} -- type: {metric["type"]}{multiline_comment}'+'\n')
        if metric["type"] in ('simple', 'cumulative', 'conversion'):
            for m in metric["type_params"]["input_measures"]:
                metrics.append(f'{metric["name"]}.{m["name"]}')
                dbdiagram_file.append(f'{m["name"]} measure'+'\n')
        elif metric["type"] == 'ratio':
            # numerator
            metrics.append(f'{metric["name"]}.{metric["type_params"]["numerator"]["name"]}')
            dbdiagram_file.append(f'{metric["type_params"]["numerator"]["name"]} metric_numerator'+'\n')
            # denominator
            metrics.append(f'{metric["name"]}.{metric["type_params"]["denominator"]["name"]}')
            dbdiagram_file.append(f'{metric["type_params"]["denominator"]["name"]} metric_denominator'+'\n')
        elif metric["type"] == 'derived':
            for met in metric["type_params"]["metrics"]:
                metrics.append(f'{metric["name"]}.{met["name"]}')
                dbdiagram_file.append(f'{met["name"]} metric'+'\n')
        else:
            print('Found something unexpected (type not in simple, ratio, derived, conversion)')
        dbdiagram_file.append('}'+'\n')

    # create pk --> fk relationships
    write_relationships(dbdiagram_file, primary_keys, foreign_keys, metrics, measures)
    txt_file = ''.join(dbdiagram_file)
    st.download_button(label='click here to download - dbdiagram all file', data=txt_file, file_name=f'dbdiagram_all_{now}.txt')
    print('Succesfully created dbdiagram.io file!')


 
def list_semantic_models(manifest):

    list_of_dicts = []
    def create_csv_dict(semantic_model, relation, field, type, description):
        return {
        "semantic_model": f"{semantic_model}",
        "relation": f"{relation}",
        "field": f"{field}",
        "type": f"{type}",
        "description": f"{description}"
        }

    # output table of semantic models
    for semantic_model in manifest['semantic_models']:
        for entity in semantic_model["entities"]:
            list_of_dicts.append(
                    create_csv_dict(
                        semantic_model["name"],
                        semantic_model["node_relation"]["alias"],
                        entity["name"],
                        entity["type"],
                        entity["description"],
                        )
                    )
        for dimension in semantic_model["dimensions"]:
            list_of_dicts.append(
                create_csv_dict(
                    semantic_model["name"],
                    semantic_model["node_relation"]["alias"],
                    dimension["name"],
                    f'dimension: {dimension["type"]}',
                    dimension["description"],
                    )
                )
        for measure in semantic_model["measures"]:
            list_of_dicts.append(
                create_csv_dict(
                    semantic_model["name"],
                    semantic_model["node_relation"]["alias"],
                    measure["name"],
                    f'measure: {measure["agg"]} - {measure["expr"]}',
                    measure["description"],
                    )
                )
    
    df = pd.DataFrame(list_of_dicts) 
    st.dataframe(df)
    print('Succesfully created semantic models file!')

metrics_dropdown = []
def list_semantic_metrics(manifest):
    list_of_dicts = []
    def create_csv_dict(metric_name, label, metrics, measures, type, description, filter):
        return {
        "metric_name": f"{metric_name}",
        "label": f"{label}",
        "metrics": f"{metrics}",
        "measures": f"{measures}",
        "type": f"{type}",
        "description": f"{description}",
        "filter": f"{filter}"
        }

    # output table of metrics
    for metric in manifest['metrics']:
        if metric["type"] in ('simple', 'cumulative', 'conversion'):
            list_of_dicts.append(
                create_csv_dict(
                    metric["name"],
                    metric["label"],
                    None,
                    [measures["name"] for measures in metric["type_params"]["input_measures"] ],
                    metric["type"],
                    metric["description"],
                    metric["filter"]
                    )
                )
        elif metric["type"] == 'ratio':
            list_of_dicts.append(
                create_csv_dict(
                    metric["name"],
                    metric["label"],
                    [metric["type_params"]["numerator"]["name"], metric["type_params"]["denominator"]["name"]],
                    [measures["name"] for measures in metric["type_params"]["input_measures"] ],
                    metric["type"],
                    metric["description"],
                    metric["filter"]
                    )
                )
        elif metric["type"] == 'derived':
            list_of_dicts.append(
                create_csv_dict(
                    metric["name"],
                    metric["label"],
                    [metrics["name"] for metrics in metric["type_params"]["metrics"] ],
                    [measures["name"] for measures in metric["type_params"]["input_measures"] ],
                    metric["type"],
                    metric["description"],
                    metric["filter"]
                    )
                )
        else:
            print('Found something unexpected (type not in simple, ratio, derived, conversion)')

    df = pd.DataFrame(list_of_dicts)
    for m in df['metric_name'].tolist():
        metrics_dropdown.append(m)
    st.dataframe(df)
    print('Succesfully created metrics file!')


st.set_page_config(
    page_title="dbt Semantic Layer ERD",
    page_icon="👋",
    layout="wide",
)

st.title("Explore your Semantic Layer ERD Relationships")
st.markdown(
    """
    dbt creates a semantic manifest artifact with useful information about your semantic models and metrics.

    This can be found in the artifacts tab from your most recent dbt Cloud job run or in development under the `target/` folder.
    """
)
st.divider()

semantic_manifest_json = st.file_uploader(label='Upload your `semantic_manifest.json` here' , type='json')

if semantic_manifest_json is not None:
    # json.load(semantic_manifest_json)
    st.write('JSON accepted!')
    st.divider()
    manifest = json.load(semantic_manifest_json)
    
    st.subheader('Semantic Layer models')
    st.write('Note: Hover over top right corner to download csv')
    list_semantic_models(manifest)
    st.divider()

    st.subheader('Semantic Layer metrics')
    st.write('Note: Hover over top right corner to download csv')
    list_semantic_metrics(manifest)
    st.divider()
    
    st.subheader('ERD - All of your Semantic Models and Metrics')
    st.markdown("""
        Read from `semantic_manifest.json` and return a dbdiagram.io ERD file of all relationships:

        - semantic models:
            - entities
            - dimensions
            - measures
        - metrics
    """
    )
    export_dbdiagram_file(manifest)
    st.markdown('Copy the contents of this file into [dbdiagram.io](https://dbdiagram.io/home) to visualize')
    st.divider()

    st.subheader('ERD - output for your Metric(s) of choice')
    st.markdown("""
        Choose from the dropdown below and an ERD file will be generated bringing in all metrics / measures / semantic models upstream from it.
        """
    )
    choose_metrics = st.multiselect("Input one or more metrics and return a dbdiagram file with the ERD related to the metric(s)", options=metrics_dropdown , default=metrics_dropdown[0])
    export_metric_dbdiagram_file(manifest, choose_metrics)
    st.markdown('Copy the contents of this file into [dbdiagram.io](https://dbdiagram.io/home) to visualize')
