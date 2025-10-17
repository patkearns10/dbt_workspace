
{%- set join_conditions = [
    {
        "orig_column": "option",
        "new_column": "option_decode",
        "tag_name": "OPT_TYPES",
        "derived": false
    },
    {
        "orig_column": "paymentclass",
        "new_column": "paymentclass_decoded",
        "tag_name": "PAYMENT",
        "derived": false
    }
] %}

-- depends_on: {{ ref('seed__decodes') }}  --- # TODO: change this to your decodes model

{{ auto_decode_v2(decode_ref=ref('seed__bcusip'), decode_ref_alias='bcusip', join_conditions=join_conditions) }}