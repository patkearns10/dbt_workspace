select 
1 as col,
JSON_OBJECT(
    ['a', 'b'],
    [TO_JSON(10), TO_JSON(['foo', 'bar'])])
    AS json_data,

-- JSON_OBJECT(null) AS json_data_fail
to_json(null) as json_null,
json_object() as json_object