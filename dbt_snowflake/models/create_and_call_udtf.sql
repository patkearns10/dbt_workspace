{{
    config(
        pre_hook= "{{ create_udtf() }}"
    )
}}

SELECT msg 
FROM TABLE(hw())
ORDER BY msg
