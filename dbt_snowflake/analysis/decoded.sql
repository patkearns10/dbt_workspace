{%- set join_conditions = [
    {
        "orig_column": "settlement_method_code",
        "new_column": "settlement_method_decoded_eq",
        "tag_name": "EQOPT_SETTLE",
        "derived": true
    },
    {
        "orig_column": "settlement_method_code",
        "new_column": "settlement_method_decoded_opt",
        "tag_name": "OPT_FUT_SETTLE",
        "derived": true
    },
    {
        "orig_column": "future_code",
        "new_column": "future_code_decoded",
        "tag_name": "FUTURE_TYPES"
    },
    {
        "orig_column": "date_conv_code",
        "new_column": "date_conv_decoded",
        "tag_name": "DAY_COUNTS"
    },
    {
        "orig_column": "option_call_put_code",
        "new_column": "option_call_put_decoded1",
        "tag_name": "CALL_OR_PUT",
        "derived": true
    },
    {
        "orig_column": "option_call_put_code",
        "new_column": "option_call_put_decoded2",
        "tag_name": "PC_TYPES",
        "derived": true
    },
    {
        "orig_column": "expiry_time_code",
        "new_column": "expiry_time_decoded",
        "tag_name": "OPTION_EXP_TIME",
        "derived": true
    },
    {
        "orig_column": "settlement_location_code",
        "new_column": "settlement_location_decoded1",
        "tag_name": "SETTLE_METHOD",
        "derived": true
    },
    {
        "orig_column": "settlement_location_code",
        "new_column": "settlement_location_decoded2",
        "tag_name": "SETTLE_LOC",
        "derived": true
    },
    {
        "orig_column": "desc_inst",
        "new_column": "desc_inst_tpr",
        "tag_name": "TPR_COLL_TYPES",
        "derived": true
    },
    {
        "orig_column": "desc_inst",
        "new_column": "desc_inst_corp",
        "tag_name": "CORP_CLASSES",
        "derived": true
    },
    {
        "orig_column": "desc_inst",
        "new_column": "desc_inst_bndlocal",
        "tag_name": "BNDLOCAL_CLASS",
        "derived": true
    },
    {
        "orig_column": "desc_inst",
        "new_column": "desc_inst_cp",
        "tag_name": "CP_CLASSES",
        "derived": true
    },
    {
        "orig_column": "desc_inst",
        "new_column": "desc_inst_sm",
        "tag_name": "SM_CT_CLASS",
        "derived": true
    },
    {
        "orig_column": "desc_inst",
        "new_column": "desc_inst_tfn",
        "tag_name": "TFN_CLASSES",
        "derived": true
    },
    {
        "orig_column": "desc_inst",
        "new_column": "desc_inst_fund",
        "tag_name": "FUND_TYPE",
        "derived": true
    },
    {
        "orig_column": "payment_convention_code",
        "new_column": "payment_convention_decoded",
        "tag_name": "ADJUST_METH",
        "derived": true
    }
] %}

{{ auto_decode(decode_ref=ref('dim_customers'), decode_ref_alias='customers', join_conditions=join_conditions)}}