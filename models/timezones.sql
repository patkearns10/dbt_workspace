select
    current_timestamp() as local_tz,  -- LA
    {{ snapshot_get_time() }} as ntz, -- UTC
    to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as local_convert_to_ntz, -- UTC

{% set now = modules.datetime.datetime.now() %}
-- UTC
'{{ now }}' as utc_now,
            
{% set local = modules.pytz.timezone('US/Eastern').localize(now) %}
-- UTC & offset
'{{ local }}' as utc_offset,

{% set d = now - modules.datetime.timedelta(hours=4) %}
-- UTC subtract hours
'{{ d }}' as local_hack
