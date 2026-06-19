{% docs __overview__ %}

# dbt Snowflake playground

This project contains long-lived Snowflake and dbt examples, demos, and Semantic Layer definitions.

## Layout

- `models/staging`: source-shaped staging models and source tests.
- `models/marts`: documented marts with model-level tests and Semantic Layer annotations.
- `models/semantic_layer`: Semantic Layer support models and saved queries that are not tied to a single mart.
- `models/demos`: experimental patterns and examples that are intentionally less production-like.
- `models/demos/general`: older standalone snippets that do not yet have a more specific home.

## Semantic Layer

Core mart semantic models use the latest YAML spec: `semantic_model`, column-level `entity` and `dimension` blocks, model-level `agg_time_dimension`, and model-scoped simple metrics. Cross-metric calculations live in the top-level `metrics` block.

{% enddocs %}
