import csv
import json
import os
import sys
import time

if len(sys.argv) == 2:
    metric_name = sys.argv[1]
else:
    metric_name = 'met_trade_footfall_daily_conversions'

# extract the current filepath
DIR_PATH = os.path.dirname(os.path.abspath(__file__))

"""
Add 'semantic_manifest.json' file to your current directory.
"""

def export_metric_dbdiagram_file():
    """create new file to import into dbdiagram.io, pulling data from semantic_layer.json"""

    ts = time.time()
    # Opening new file and JSON file
    with open(os.path.join(DIR_PATH, f'dbdiagram_metric_{ts}.txt'), 'w') as dbdiagram_metric_file, open(os.path.join(DIR_PATH, 'semantic_manifest.json')) as semantic_manifest_file:

        # returns JSON object as a dictionary
        manifest = json.load(semantic_manifest_file)
        
        primary_keys = []
        foreign_keys = []
        metrics = []
        measures = []

        # get metric - measure matches
        for metric in manifest['metrics']:
            if metric["name"] == metric_name:        
                dbdiagram_metric_file.write(f'Table {metric["name"]}'+'\n')
                dbdiagram_metric_file.write('{')
                dbdiagram_metric_file.write('\n')
                dbdiagram_metric_file.write(f'Note: "{metric["description"]}"'+'\n')
                if metric["type"] == 'simple':
                    metrics.append(f'{metric["name"]}.{metric["type_params"]["measure"]["name"]}')
                    dbdiagram_metric_file.write(f'{metric["type_params"]["measure"]["name"]} measure'+'\n')
                    dbdiagram_metric_file.write('}')
                    dbdiagram_metric_file.write('\n')
                elif metric["type"] == 'ratio':
                    # numerator
                    metrics.append(f'{metric["name"]}.{metric["type_params"]["numerator"]["name"]}')
                    dbdiagram_metric_file.write(f'{metric["type_params"]["numerator"]["name"]} measure_numerator'+'\n')
                    # denominator
                    metrics.append(f'{metric["name"]}.{metric["type_params"]["denominator"]["name"]}')
                    dbdiagram_metric_file.write(f'{metric["type_params"]["denominator"]["name"]} measure_denominator'+'\n')
                    dbdiagram_metric_file.write('}')
                    dbdiagram_metric_file.write('\n')
                    for m in metrics:
                        for metric in manifest['metrics']:
                            if m.split('.')[1] == metric["name"]:
                                dbdiagram_metric_file.write(f'Table {metric["name"]}'+'\n')
                                dbdiagram_metric_file.write('{')
                                dbdiagram_metric_file.write('\n')
                                dbdiagram_metric_file.write(f'Note: "{metric["description"]}"'+'\n')
                                if metric["type"] == 'simple':
                                    metrics.append(f'{metric["name"]}.{metric["type_params"]["measure"]["name"]}')
                                    dbdiagram_metric_file.write(f'{metric["type_params"]["measure"]["name"]} measure'+'\n')
                                    dbdiagram_metric_file.write('}')
                                    dbdiagram_metric_file.write('\n')
                                elif metric["type"] == 'ratio':
                                    # numerator
                                    metrics.append(f'{metric["name"]}.{metric["type_params"]["numerator"]["name"]}')
                                    dbdiagram_metric_file.write(f'{metric["type_params"]["numerator"]["name"]} measure_numerator'+'\n')
                                    # denominator
                                    metrics.append(f'{metric["name"]}.{metric["type_params"]["denominator"]["name"]}')
                                    dbdiagram_metric_file.write(f'{metric["type_params"]["denominator"]["name"]} measure_denominator'+'\n')
                                    dbdiagram_metric_file.write('}')
                                    dbdiagram_metric_file.write('\n')
                                else:
                                    print('todo: other types of metrics (cumulative, conversion, derived')

        # get measures to semantic model matches
        tables_built = []
        for semantic_model in manifest['semantic_models']:
            for measure in semantic_model["measures"]:
                for metric in metrics:
                    if metric.split('.')[1] == measure["name"] and semantic_model["name"] not in tables_built:
                        dbdiagram_metric_file.write(f'Table {semantic_model["name"]}'+'\n')
                        dbdiagram_metric_file.write('{')
                        dbdiagram_metric_file.write('\n')
                        dbdiagram_metric_file.write(f'Note: "{semantic_model["node_relation"]["alias"]} -- {semantic_model["description"]}"'+'\n')
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
                        dbdiagram_metric_file.write('}')
                        dbdiagram_metric_file.write('\n')
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
                dbdiagram_metric_file.write('{')
                dbdiagram_metric_file.write('\n')
                dbdiagram_metric_file.write(f'Note: "{semantic_model["node_relation"]["alias"]} -- {semantic_model["description"]}"'+'\n')
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
                dbdiagram_metric_file.write('}')
                dbdiagram_metric_file.write('\n')
                tables_built.append(semantic_model["name"])

        # create pk --> fk relationships
        dbdiagram_metric_file.write('\n')
        for primary_key in list(set(primary_keys)):
            for foreign_key in list(set(foreign_keys)):
                # Ref: sem_date.date_key - sem_sale_item.date_key
                if primary_key.split('.')[1] == foreign_key.split('.')[1]:
                    if primary_key.split('.')[1] == 'date_key':
                        dbdiagram_metric_file.write(f'Ref: {primary_key} - {foreign_key}'+'\n')
                    else:
                        dbdiagram_metric_file.write(f'Ref: {primary_key} < {foreign_key}'+'\n')
        for metric in list(set(metrics)):
            for measure in list(set(measures)):
                if metric.split('.')[1] == measure.split('.')[1]:
                    dbdiagram_metric_file.write(f'Ref: {metric} - {measure}'+'\n')
        for metric in list(set(metrics)):
            for m in list(set(metrics)):
                if metric.split('.')[1] == m.split('.')[0]:
                    dbdiagram_metric_file.write(f'Ref: {metric} - {m}'+'\n')

    print('Succesfully created metric dbdiagram.io file!')

def export_dbdiagram_file():
    """create new file to import into dbdiagram.io, pulling data from semantic_layer.json"""

    ts = time.time()
    # Opening new file and JSON file
    with open(os.path.join(DIR_PATH, f'dbdiagram_all_{ts}.txt'), 'w') as dbdiagram_file, open(os.path.join(DIR_PATH, 'semantic_manifest.json')) as semantic_manifest_file:

        # returns JSON object as a dictionary
        manifest = json.load(semantic_manifest_file)
        
        primary_keys = []
        foreign_keys = []
        metrics = []
        measures = []
        # print table format for dbdiagram.io
        for semantic_model in manifest['semantic_models']:
            dbdiagram_file.write(f'Table {semantic_model["name"]}'+'\n')
            dbdiagram_file.write('{')
            dbdiagram_file.write('\n')
            dbdiagram_file.write(f'Note: "{semantic_model["node_relation"]["alias"]} -- {semantic_model["description"]}"'+'\n')
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
            dbdiagram_file.write('}')
            dbdiagram_file.write('\n')

        # create metrics tables
        dbdiagram_file.write('\n')
        for metric in manifest['metrics']:
            dbdiagram_file.write(f'Table {metric["name"]}'+'\n')
            dbdiagram_file.write('{')
            dbdiagram_file.write(f'Note: "{metric["description"]}"'+'\n')
            if metric["type"] == 'simple':
                metrics.append(f'{metric["name"]}.{metric["type_params"]["measure"]["name"]}')
                dbdiagram_file.write(f'{metric["type_params"]["measure"]["name"]} measure'+'\n')
            elif metric["type"] == 'ratio':
                # numerator
                metrics.append(f'{metric["name"]}.{metric["type_params"]["numerator"]["name"]}')
                dbdiagram_file.write(f'{metric["type_params"]["numerator"]["name"]} measure_numerator'+'\n')
                # denominator
                metrics.append(f'{metric["name"]}.{metric["type_params"]["denominator"]["name"]}')
                dbdiagram_file.write(f'{metric["type_params"]["denominator"]["name"]} measure_denominator'+'\n')
            else:
                print('todo: other types of metrics (cumulative, conversion, derived')
            dbdiagram_file.write('}')

        # create pk --> fk relationships
        dbdiagram_file.write('\n')
        for primary_key in primary_keys:
            for foreign_key in foreign_keys:
                # Ref: sem_date.date_key - sem_sale_item.date_key
                if primary_key.split('.')[1] == foreign_key.split('.')[1]:
                    if primary_key.split('.')[1] == 'date_key':
                        dbdiagram_file.write(f'Ref: {primary_key} - {foreign_key}'+'\n')
                    else:
                        dbdiagram_file.write(f'Ref: {primary_key} < {foreign_key}'+'\n')
        for metric in metrics:
            for measure in measures:
                if metric.split('.')[1] == measure.split('.')[1]:
                    dbdiagram_file.write(f'Ref: {metric} - {measure}'+'\n')

    print('Succesfully created dbdiagram.io file!')
    
def list_fields_and_metrics():
    """create two csv files: semantic model fields & metrics."""

    ts = time.time()
    # Opening new file and JSON file
    with open(os.path.join(DIR_PATH, f'semantic_models_{ts}.csv'), 'w') as semantic_models_file, open(os.path.join(DIR_PATH, f'semantic_metrics_{ts}.csv'), 'w') as semantic_metrics_file, open(os.path.join(DIR_PATH, 'semantic_manifest.json')) as semantic_manifest_file:

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

        # print table format for dbdiagram.io
        for semantic_model in manifest['semantic_models']:
            for entity in semantic_model["entities"]:
                writer.writerow(
                        create_csv_dict(
                            semantic_model["name"],
                            semantic_model["node_relation"]["relation_name"],
                            entity["name"],
                            entity["type"],
                            entity["description"],
                            )
                        )
            for dimension in semantic_model["dimensions"]:
                writer.writerow(
                    create_csv_dict(
                        semantic_model["name"],
                        semantic_model["node_relation"]["relation_name"],
                        dimension["name"],
                        f'dimension: {dimension["type"]}',
                        dimension["description"],
                        )
                    )
            for measure in semantic_model["measures"]:
                writer.writerow(
                    create_csv_dict(
                        semantic_model["name"],
                        semantic_model["node_relation"]["relation_name"],
                        measure["name"],
                        f'measure: {measure["agg"]} - {measure["expr"]}',
                        measure["description"],
                        )
                    )
        print('Succesfully created semantic models file!')

        # metrics csv
        metric_fields = ['metric_name', 'label', 'measures', 'type', 'description', 'filter']
        def create_csv_dict(metric_name, label, measures, type, description, filter):
            return {
            "metric_name": f"{metric_name}",
            "label": f"{label}",
            "measures": f"{measures}",
            "type": f"{type}",
            "description": f"{description}",
            "filter": f"{filter}"
            }

        # creating a csv dict writer object
        metric_writer = csv.DictWriter(semantic_metrics_file, fieldnames=metric_fields)
        metric_writer.writeheader()


        for metric in manifest['metrics']:
            if metric["type"] == 'simple':
                metric_writer.writerow(
                    create_csv_dict(
                        metric["name"],
                        metric["label"],
                        [metric["type_params"]["measure"]["name"]],
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
                        metric["type"],
                        metric["description"],
                        metric["filter"]
                        )
                    )
            else:
                print('todo: other types of metrics (cumulative, conversion, derived')

    print('Succesfully created metrics file!')


if __name__ == '__main__':
    # export_metric_dbdiagram_file()
    list_fields_and_metrics()
    # export_dbdiagram_file()