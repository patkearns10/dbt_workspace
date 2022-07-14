CREATE OR REPLACE PROCEDURE UTILITY.EDW_BATCH_FAILED_PROC(PROCESS_NAME VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
--EXECUTE AS OWNER
AS '
	var my_sql_command="select process_id,process_name from utility.edw_process_info where process_name=''"+PROCESS_NAME+"''";
	var stmt=snowflake.createStatement( {sqlText: my_sql_command} );
	var res=stmt.execute();
	while (res.next())  {
	var v_process_id = res.getColumnValue(1);
	var v_process_name = res.getColumnValue(2);
    
    //identify the maximum batch_id FOR the given process id
    var max_batchid_cmd="select max(batch_id) from utility.EDW_PROCESS_BATCH_CTL where batch_status=''R'' and process_id="+v_process_id;
	var max_batchid_stmt=snowflake.createStatement( {sqlText: max_batchid_cmd} );
	var max_batch_id_res=max_batchid_stmt.execute();
	max_batch_id_res.next()
	var v_max_batch_id = max_batch_id_res.getColumnValue(1);

	//"UPDATE the running batch_id TO failed status"
    
    var upd_cmd="update utility.EDW_PROCESS_BATCH_CTL set batch_status=''F'',EDW_UPD_DTTM=CURRENT_TIMESTAMP,END_DTTM=CURRENT_TIMESTAMP,COMMENTS=''BATCH FAILED'' WHERE BATCH_ID="+v_max_batch_id+" AND PROCESS_ID="+v_process_id+" and batch_status=''R''";
    var upd_cmd_stmt=snowflake.createStatement( {sqlText: upd_cmd} );
    var queryText = upd_cmd_stmt.getSqlText();
    var upd_cmd_res=upd_cmd_stmt.execute();
    
    

	//while loop ends
    }
return  "process_id "+v_process_id+"  is updated to failed status" ;
';