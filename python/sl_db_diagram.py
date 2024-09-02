import csv
from datetime import datetime
import json
import os
import sys


if len(sys.argv) == 2:
    arg_metric_name = sys.argv[1]
else:
    arg_metric_name = 'met_trade_agg_units_per_transaction_yoy'
arg_metric_list = arg_metric_name.split(",")

"""
Add 'semantic_manifest.json' file to your current directory.
"""

# extract the current filepath
DIR_PATH = os.path.dirname(os.path.abspath(__file__))
now = datetime.now().strftime('%Y.%m.%d.%H.%M.%S')
multiline_comment = "'''"
# my_list = my_string.split(",")

# some helper macros
def tables_for_models(manifest, dbdiagram_file, primary_keys, foreign_keys, measures):
    # semantic model: output table format for dbdiagram.io
    for semantic_model in manifest['semantic_models']:
        dbdiagram_file.write(f'Table {semantic_model["name"]}'+'\n')
        dbdiagram_file.write('{'+'\n')
        dbdiagram_file.write(f'Note: {multiline_comment}{semantic_model["node_relation"]["alias"]} -- {semantic_model["description"]}{multiline_comment}'+'\n')
        for entity in semantic_model["entities"]:
            if entity["type"] == 'primary':
                primary_keys.append(f'{semantic_model["name"]}.{entity["name"]}')
                dbdiagram_file.write(f'{entity["name"]} primary [primary key]'+'\n')
            else:
                foreign_keys.append(f'{semantic_model["name"]}.{entity["name"]}')
                dbdiagram_file.write(f'{entity["name"]} {entity["type"]}'+'\n')
        for dimension in semantic_model["dimensions"]:
            dbdiagram_file.write(f'{dimension["name"]} dimension'+'\n')
        for measure in semantic_model["measures"]:
            measures.append(f'{semantic_model["name"]}.{measure["name"]}')
            dbdiagram_file.write(f'{measure["name"]} measure'+'\n')
        dbdiagram_file.write('}'+'\n')

def tables_for_metrics(manifest, dbdiagram_file, metrics):
    # metrics: output table format for dbdiagram.io
    dbdiagram_file.write('\n')
    for metric in manifest['metrics']:
        dbdiagram_file.write(f'Table {metric["name"]}'+'\n')
        dbdiagram_file.write('{'+'\n')
        dbdiagram_file.write(f'Note: {multiline_comment}{metric["description"]} -- type: {metric["type"]}{multiline_comment}'+'\n')
        if metric["type"] in ('simple', 'cumulative', 'conversion'):
            for m in metric["type_params"]["input_measures"]:
                metrics.append(f'{metric["name"]}.{m["name"]}')
                dbdiagram_file.write(f'{m["name"]} measure'+'\n')
        elif metric["type"] == 'ratio':
            # numerator
            metrics.append(f'{metric["name"]}.{metric["type_params"]["numerator"]["name"]}')
            dbdiagram_file.write(f'{metric["type_params"]["numerator"]["name"]} measure_numerator'+'\n')
            # denominator
            metrics.append(f'{metric["name"]}.{metric["type_params"]["denominator"]["name"]}')
            dbdiagram_file.write(f'{metric["type_params"]["denominator"]["name"]} measure_denominator'+'\n')
        elif metric["type"] == 'derived':
            for met in metric["type_params"]["metrics"]:
                metrics.append(f'{metric["name"]}.{met["name"]}')
                dbdiagram_file.write(f'{met["name"]} metric'+'\n')
        else:
            print('Found something unexpected (type not in simple, ratio, derived, conversion)')
        dbdiagram_file.write('}'+'\n')

def write_relationships(file_name, primary_keys, foreign_keys, metrics, measures):
    # create pk --> fk relationships
    file_name.write('\n')
    for primary_key in list(set(primary_keys)):
        for foreign_key in list(set(foreign_keys)):
            # Ref: sem_date.date_key - sem_sale_item.date_key
            if primary_key.split('.')[1] == foreign_key.split('.')[1]:
                if primary_key.split('.')[1] == 'date_key':
                    file_name.write(f'Ref: {primary_key} - {foreign_key}'+'\n')
                else:
                    file_name.write(f'Ref: {primary_key} < {foreign_key}'+'\n')
    for metric in list(set(metrics)):
        for m in list(set(metrics)):
            if metric.split('.')[1] == m.split('.')[0] and metric != m:
                file_name.write(f'Ref: {metric} - {m}'+'\n')
    for metric in list(set(metrics)):
        for measure in list(set(measures)):
            if metric.split('.')[1] == measure.split('.')[1] and metric != measure:
                file_name.write(f'Ref: {metric} - {measure}'+'\n')

def recursive_metric_finder(manifest, metrics, file_name, metrics_consumed):
    # metrics can reference other metrics, so for those types, recursively find the metrics
    for r_metric in metrics:
        for manifest_metric in manifest['metrics']:
            if manifest_metric["name"] == r_metric.split('.')[1] and manifest_metric["name"] not in metrics_consumed:
                file_name.write(f'Table {manifest_metric["name"]}'+'\n')
                file_name.write('{'+'\n')
                file_name.write(f'Note: {multiline_comment}{manifest_metric["description"]} -- type: {manifest_metric["type"]}{multiline_comment}'+'\n')
                if manifest_metric["type"] in ('simple', 'cumulative', 'conversion') and manifest_metric["name"] not in metrics_consumed:
                    # these metrics only depend on measures
                    for mm in manifest_metric["type_params"]["input_measures"]:
                        metrics.append(f'{manifest_metric["name"]}.{mm["name"]}')
                        file_name.write(f'{mm["name"]} measure'+'\n')
                    file_name.write('}'+'\n')
                    metrics_consumed.append(manifest_metric["name"])
                    break
                elif manifest_metric["type"] == 'derived' and manifest_metric["name"] not in metrics_consumed:
                    # depends on one or many metrics
                    for mmet in manifest_metric["type_params"]["metrics"]:
                        metrics.append(f'{manifest_metric["name"]}.{mmet["name"]}')
                        file_name.write(f'{mmet["name"]} metric'+'\n')
                        metrics_consumed.append(manifest_metric["name"])
                    file_name.write('}'+'\n')
                elif manifest_metric["type"] == 'ratio' and manifest_metric["name"] not in metrics_consumed:
                    # numerator metric
                    metrics.append(f'{manifest_metric["name"]}.{manifest_metric["type_params"]["numerator"]["name"]}')
                    file_name.write(f'{manifest_metric["type_params"]["numerator"]["name"]} metric_numerator'+'\n')
                    # denominator metric
                    metrics.append(f'{manifest_metric["name"]}.{manifest_metric["type_params"]["denominator"]["name"]}')
                    file_name.write(f'{manifest_metric["type_params"]["denominator"]["name"]} metric_denominator'+'\n')
                    file_name.write('}'+'\n')
                    metrics_consumed.append(manifest_metric["type_params"]["numerator"]["name"])
                    metrics_consumed.append(manifest_metric["type_params"]["denominator"]["name"])
                else:
                    print('Found something unexpected (type not in simple, ratio, derived, conversion)')

def export_metric_dbdiagram_file():
    """
    Summary:
        Input one or more metrics and return a dbdiagram file. https://dbdiagram.io/home
    Steps in function:
        Import semantic_manifest.json from local directory.
        Starting with a provided metric (input directly on line 12 or as a comma separated string with no spaces),
        traverse the semantic json and bring in all metrics / measures / semantic models upstream from it.
        create tables in dbml syntax
        create references in dbml syntax
        export dbml file
    """

    # Opening new file and JSON file
    with open(os.path.join(DIR_PATH, f'dbdiagram_metric_{now}.dbml'), 'w') as dbdiagram_metric_file, open(os.path.join(DIR_PATH, 'semantic_manifest.json')) as semantic_manifest_file:

        # returns JSON object as a dictionary
        manifest = json.load(semantic_manifest_file)
        
        primary_keys = []
        foreign_keys = []
        metrics = []
        measures = []
        metrics_consumed = []
        # get metric - measure matches
        for arg_metric in arg_metric_list:
            for metric in manifest['metrics']:
                if metric["name"] == arg_metric and metric["name"] not in metrics_consumed:       
                    dbdiagram_metric_file.write(f'Table {metric["name"]}'+'\n')
                    dbdiagram_metric_file.write('{'+'\n')
                    dbdiagram_metric_file.write(f'Note: {multiline_comment}{metric["description"]} -- type: {metric["type"]}{multiline_comment}'+'\n')
                    if metric["type"] in ('simple', 'cumulative', 'conversion') and metric["name"] not in metrics_consumed:
                        for m in metric["type_params"]["input_measures"]:
                            metrics.append(f'{metric["name"]}.{m["name"]}')
                            dbdiagram_metric_file.write(f'{m["name"]} measure'+'\n')
                        dbdiagram_metric_file.write('}'+'\n')
                        metrics_consumed.append(metric["name"])

                    elif metric["type"] == 'derived' and metric["name"] not in metrics_consumed:
                        for met in metric["type_params"]["metrics"]:
                            metrics.append(f'{metric["name"]}.{met["name"]}')
                            dbdiagram_metric_file.write(f'{met["name"]} metric'+'\n')
                        dbdiagram_metric_file.write('}'+'\n')
                        metrics_consumed.append(metric["name"])
                        recursive_metric_finder(manifest, metrics, dbdiagram_metric_file, metrics_consumed)

                    elif metric["type"] == 'ratio' and metric["name"] not in metrics_consumed:
                        # numerator
                        metrics.append(f'{metric["name"]}.{metric["type_params"]["numerator"]["name"]}')
                        dbdiagram_metric_file.write(f'{metric["type_params"]["numerator"]["name"]} measure_numerator'+'\n')
                        # denominator
                        metrics.append(f'{metric["name"]}.{metric["type_params"]["denominator"]["name"]}')
                        dbdiagram_metric_file.write(f'{metric["type_params"]["denominator"]["name"]} measure_denominator'+'\n')
                        dbdiagram_metric_file.write('}'+'\n')
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
                        dbdiagram_metric_file.write(f'Table {semantic_model["name"]}'+'\n')
                        dbdiagram_metric_file.write('{'+'\n')
                        dbdiagram_metric_file.write(f'Note: {multiline_comment}{semantic_model["node_relation"]["alias"]} -- {semantic_model["description"]}{multiline_comment}'+'\n')
                        for entity in semantic_model["entities"]:
                            if entity["type"] == 'primary':
                                primary_keys.append(f'{semantic_model["name"]}.{entity["name"]}')
                                dbdiagram_metric_file.write(f'{entity["name"]} primary [primary key]'+'\n')
                            else:
                                foreign_keys.append(f'{semantic_model["name"]}.{entity["name"]}')
                                dbdiagram_metric_file.write(f'{entity["name"]} {entity["type"]}'+'\n')
                        for dimension in semantic_model["dimensions"]:
                            dbdiagram_metric_file.write(f'{dimension["name"]} dimension'+'\n')
                        for measure in semantic_model["measures"]:
                            measures.append(f'{semantic_model["name"]}.{measure["name"]}')
                            dbdiagram_metric_file.write(f'{measure["name"]} measure'+'\n')
                        dbdiagram_metric_file.write('}'+'\n')
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
                dbdiagram_metric_file.write(f'Table {semantic_model["name"]}'+'\n')
                dbdiagram_metric_file.write('{'+'\n')
                dbdiagram_metric_file.write(f'Note: {multiline_comment}{semantic_model["node_relation"]["alias"]} -- {semantic_model["description"]}{multiline_comment}'+'\n')
                for entity in semantic_model["entities"]:
                    if entity["type"] == 'primary':
                        primary_keys.append(f'{semantic_model["name"]}.{entity["name"]}')
                        dbdiagram_metric_file.write(f'{entity["name"]} primary [primary key]'+'\n')
                    else:
                        foreign_keys.append(f'{semantic_model["name"]}.{entity["name"]}')
                        dbdiagram_metric_file.write(f'{entity["name"]} {entity["type"]}'+'\n')
                for dimension in semantic_model["dimensions"]:
                    dbdiagram_metric_file.write(f'{dimension["name"]} dimension'+'\n')
                for measure in semantic_model["measures"]:
                    measures.append(f'{semantic_model["name"]}.{measure["name"]}')
                    dbdiagram_metric_file.write(f'{measure["name"]} measure'+'\n')
                dbdiagram_metric_file.write('}'+'\n')
                tables_built.append(semantic_model["name"])

        # create pk --> fk relationships
        write_relationships(dbdiagram_metric_file, primary_keys, foreign_keys, metrics, measures)

    print('Succesfully created metric dbdiagram.io file!')

def export_dbdiagram_file():
    """
    Summary:
        Read from entire semantic_manifest.json and return a dbdiagram file of all connections. https://dbdiagram.io/home
    Steps in function:
        Import semantic_manifest.json from local directory.
        Traverse the semantic json and bring in all metrics / measures / semantic models.
        create tables in dbml syntax
        create references in dbml syntax
        export dbml file
    """

    # Opening new file and JSON file
    with open(os.path.join(DIR_PATH, f'dbdiagram_all_{now}.dbml'), 'w') as dbdiagram_file, open(os.path.join(DIR_PATH, 'semantic_manifest.json')) as semantic_manifest_file:

        # returns JSON object as a dictionary
        manifest = json.load(semantic_manifest_file)
        
        primary_keys = []
        foreign_keys = []
        metrics = []
        measures = []

        # semantic model: output table format for dbdiagram.io
        tables_for_models(manifest, dbdiagram_file, primary_keys, foreign_keys, measures)

        # create metrics tables
        tables_for_metrics(manifest, dbdiagram_file, metrics)

        # create pk --> fk relationships
        write_relationships(dbdiagram_file, primary_keys, foreign_keys, metrics, measures)

    print('Succesfully created dbdiagram.io file!')
    

def export_semantic_model_dbdiagram_file():
    """
    Summary:
        Read from entire semantic_manifest.json and return a dbdiagram file of semantic model connections. https://dbdiagram.io/home
    Steps in function:
        Import semantic_manifest.json from local directory.
        Traverse the semantic json and bring in all semantic models.
        create tables in dbml syntax
        create references in dbml syntax
        export dbml file
    """

    # Opening new file and JSON file
    with open(os.path.join(DIR_PATH, f'dbdiagram_semantic_models_{now}.dbml'), 'w') as dbdiagram_file, open(os.path.join(DIR_PATH, 'semantic_manifest.json')) as semantic_manifest_file:

        # returns JSON object as a dictionary
        manifest = json.load(semantic_manifest_file)
        
        primary_keys = []
        foreign_keys = []
        metrics = []
        measures = []

        # semantic model: output table format for dbdiagram.io
        tables_for_models(manifest, dbdiagram_file, primary_keys, foreign_keys, measures)

        # create pk --> fk relationships
        write_relationships(dbdiagram_file, primary_keys, foreign_keys, metrics=[], measures=[])

    print('Succesfully created dbdiagram.io file!')    
def list_fields_and_metrics():
    """create two csv files: semantic model fields & metrics."""

    # Opening new file and JSON file
    with open(os.path.join(DIR_PATH, f'semantic_models_{now}.csv'), 'w') as semantic_models_file, open(os.path.join(DIR_PATH, f'semantic_metrics_{now}.csv'), 'w') as semantic_metrics_file, open(os.path.join(DIR_PATH, 'semantic_manifest.json')) as semantic_manifest_file:

        # returns JSON object as a dictionary
        manifest = json.load(semantic_manifest_file)
        
        # semantic models csv
        fields = ['semantic_model', 'relation', 'field', 'type', 'description']
        def create_csv_dict(semantic_model, relation, field, type, description):
            return {
            "semantic_model": f"{semantic_model}",
            "relation": f"{relation}",
            "field": f"{field}",
            "type": f"{type}",
            "description": f"{description}"
            }

        # creating a csv dict writer object
        writer = csv.DictWriter(semantic_models_file, fieldnames=fields)
        writer.writeheader()

        # output table format for dbdiagram.io for semantic models
        for semantic_model in manifest['semantic_models']:
            for entity in semantic_model["entities"]:
                writer.writerow(
                        create_csv_dict(
                            semantic_model["name"],
                            semantic_model["node_relation"]["alias"],
                            entity["name"],
                            entity["type"],
                            entity["description"],
                            )
                        )
            for dimension in semantic_model["dimensions"]:
                writer.writerow(
                    create_csv_dict(
                        semantic_model["name"],
                        semantic_model["node_relation"]["alias"],
                        dimension["name"],
                        f'dimension: {dimension["type"]}',
                        dimension["description"],
                        )
                    )
            for measure in semantic_model["measures"]:
                writer.writerow(
                    create_csv_dict(
                        semantic_model["name"],
                        semantic_model["node_relation"]["alias"],
                        measure["name"],
                        f'measure: {measure["agg"]}',
                        f'{measure["description"]} | expr: {measure["expr"]}',
                        )
                    )
        print('Succesfully created semantic models file!')

        # metrics csv
        metric_fields = ['metric_name', 'label', 'metrics', 'measures', 'type', 'description', 'filter']
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

        # creating a csv dict writer object
        metric_writer = csv.DictWriter(semantic_metrics_file, fieldnames=metric_fields)
        metric_writer.writeheader()

        # output table format for dbdiagram.io for metrics
        for metric in manifest['metrics']:
            if metric["type"] in ('simple', 'cumulative', 'conversion'):
                metric_writer.writerow(
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
                metric_writer.writerow(
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
                metric_writer.writerow(
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

    print('Succesfully created metrics file!')


if __name__ == '__main__':
    export_semantic_model_dbdiagram_file()
    export_metric_dbdiagram_file()
    export_dbdiagram_file()
    list_fields_and_metrics()