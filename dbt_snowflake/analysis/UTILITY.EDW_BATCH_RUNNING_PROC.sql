CREATE OR REPLACE PROCEDURE UTILITY.EDW_BATCH_RUNNING_PROC(PROCESS_NAME VARCHAR(16777216))
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
    var max_batchid_cmd="select max(batch_id) from utility.EDW_PROCESS_BATCH_CTL where batch_status=''Q'' and process_id="+v_process_id;
	var max_batchid_stmt=snowflake.createStatement( {sqlText: max_batchid_cmd} );
	var max_batch_id_res=max_batchid_stmt.execute();
	max_batch_id_res.next()
	var v_max_batch_id = max_batch_id_res.getColumnValue(1);
    
    //identify the number OF records IN Q status FOR the given process ID
    var count_of_queue_cmd="select count(*) from utility.EDW_PROCESS_BATCH_CTL where batch_status=''Q'' and process_id="+v_process_id;
	var count_of_queue_stmt=snowflake.createStatement( {sqlText: count_of_queue_cmd} );
	var count_of_queue_stmt_res=count_of_queue_stmt.execute();
	count_of_queue_stmt_res.next()
	var v_count_of_queue = count_of_queue_stmt_res.getColumnValue(1);

    try {
    if ( ( v_count_of_queue == 1 ) ) 
    {
    
    //UPDATE the queue batch_id TO running status
    
    var upd_cmd="update utility.EDW_PROCESS_BATCH_CTL set batch_status=''R'',EDW_UPD_DTTM=CURRENT_TIMESTAMP,START_DTTM=CURRENT_TIMESTAMP,COMMENTS=''BATCH RUNNING'' WHERE BATCH_ID="+v_max_batch_id+" AND PROCESS_ID="+v_process_id+" and batch_status=''Q''";
    var upd_cmd_stmt=snowflake.createStatement( {sqlText: upd_cmd} );
    var queryText = upd_cmd_stmt.getSqlText();
    var upd_cmd_res=upd_cmd_stmt.execute();
        	
    //IF block ending
    }
    
    else if (v_count_of_queue == 0)
    {
    
    throw  " process_id "+v_process_id+" does not have any queues ";
    //else if block ending
    
    }
    else
    
    {
     throw  " process_id "+v_process_id+" have multiple queues ";
    //ELSE block ending
    }
    
    //try block ending
    }
    
    catch (err) {
        return "Error: " + err;
    }
    
    
	

	
	//while loop ending 
    }

return " Updated batch_id "+v_max_batch_id+" for process_id "+v_process_id+ " into running status";
';