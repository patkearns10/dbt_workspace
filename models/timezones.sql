select
    current_timestamp() as local_tz,  -- LA
    {{ snapshot_get_time() }} as ntz, -- UTC
    to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as local_convert_to_ntz -- UTC
