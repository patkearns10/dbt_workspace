#!/usr/bin/env python

import subprocess
import json
import os
import logging
import errno
import yaml
import argparse


# TODO add ability to define alias for sources

STAGING_FOLDER = "staging"
SOURCE_FILE_NAME = "_source.yml"
# staging model is in the form of <PREFIX><source_name><SEPARATOR_STAGING><table_name>.sql
PREFIX = "stg_"
SEPARATOR_STAGING = "__"

logging.basicConfig(level=logging.INFO)


# Setup of the different commands and their arguments
parser = argparse.ArgumentParser()

subparsers = parser.add_subparsers(
    title="subcommands", description="valid subcommands", dest="command"
)
generate_source = subparsers.add_parser("generate_source")
generate_staging = subparsers.add_parser("generate_staging")
generate_source_staging = subparsers.add_parser("generate_source_staging")

generate_source.add_argument("database_name")
generate_source.add_argument("schema_name")
generate_source.add_argument("--overwrite", action="store_true")

generate_staging.add_argument("source_name")
generate_staging.add_argument("--overwrite", action="store_true")

generate_source_staging.add_argument("database_name")
generate_source_staging.add_argument("schema_name")
generate_source_staging.add_argument("--overwrite", action="store_true")
args = parser.parse_args()


def generate_sql_source(source_name, table_name):

    outp = (
        subprocess.run(
            [
                "dbt",
                "--log-format",
                "json",
                "run-operation",
                "generate_base_model",
                "--args",
                f'{{"source_name": "{source_name}", "table_name": "{table_name}"}}',
            ],
            capture_output=True,
            text=True,
        )
        .stdout.strip()
        .split('"}}')
    )

    # The text we want is alway the last part of the logs
    generated_yaml = outp[-1]
    return generated_yaml


def save_sql_source(source_name, table_name, overwrite=False):

    filename = f"{PREFIX}{source_name}{SEPARATOR_STAGING}{table_name}.sql"
    filepath = f"./models/{STAGING_FOLDER}/{source_name}/{filename}"

    if os.path.exists(filepath):
        if not overwrite:
            logging.info(f"The file {filename} already exists and was kept unchanged")
            return
        else:
            logging.warning(
                f"The file {filename} already existed and will be overwritten"
            )

    sql = generate_sql_source(source_name, table_name)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w") as f:
        f.write(sql)
    logging.info(f"The file {filename} has been written to:")
    logging.info(f"{filepath}")


def generate_yml_sources(schema_name, database_name):

    outp = (
        subprocess.run(
            [
                "dbt",
                "--log-format",
                "json",
                "run-operation",
                "generate_source",
                "--args",
                f'{{"schema_name": "{schema_name}", "database_name": "{database_name}"}}',
            ],
            capture_output=True,
            text=True,
        )
        .stdout.strip()
        .split("}")
    )

    # The text we want is alway the last part of the logs
    generated_sql = outp[-1]
    return generated_sql


def save_yml_sources(schema_name, database_name, overwrite=False):

    filepath = f"./models/{STAGING_FOLDER}/{schema_name}/{SOURCE_FILE_NAME}"

    if os.path.exists(filepath):
        if not overwrite:
            logging.info(
                f"The file {schema_name}/{SOURCE_FILE_NAME} already exists and was kept unchanged"
            )
            return
        else:
            logging.warning(
                f"The file {schema_name}/{SOURCE_FILE_NAME} already existed and will be overwritten"
            )

    yml_data = generate_yml_sources(schema_name, database_name)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w") as f:
        f.write(yml_data)
    logging.info(f"The file {schema_name}/{SOURCE_FILE_NAME} has been written to:")
    logging.info(f"{filepath}")


def read_yml_sources(schema_name):

    yml_file_path = f"./models/{STAGING_FOLDER}/{schema_name}/{SOURCE_FILE_NAME}"
    if not os.path.exists(yml_file_path):
        FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), yml_file_path)

    source_tables_list = []

    with open(yml_file_path) as yml_file:
        sources_list = yaml.load(yml_file, Loader=yaml.FullLoader)

    for source_table in sources_list["sources"][0]["tables"]:
        source_tables_list.append(
            {
                "source_name": sources_list["sources"][0]["name"],
                "table_name": source_table["name"],
            }
        )

    logging.info(
        f"Read {len(source_tables_list)} tables from {schema_name}/{SOURCE_FILE_NAME}"
    )
    return source_tables_list


if __name__ == "__main__":

    if args.command == "generate_source_staging":
        save_yml_sources(args.schema_name, args.database_name, args.overwrite)
        source_tables = read_yml_sources(args.schema_name)

        for source_table in source_tables:
            save_sql_source(
                source_table["source_name"], source_table["table_name"], args.overwrite
            )

    elif args.command == "generate_source":
        save_yml_sources(args.schema_name, args.database_name, args.overwrite)

    elif args.command == "generate_staging":
        source_tables = read_yml_sources(args.source_name)

        for source_table in source_tables:
            save_sql_source(
                source_table["source_name"], source_table["table_name"], args.overwrite
            )