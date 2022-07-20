CREATE OR REPLACE PROCEDURE UTILITY.EDW_BATCH_QUEUE_PROC(PROCESS_NAME VARCHAR(16777216), HWM_DTTM VARCHAR(16777216))
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
    //Current Process_id with Queue count
	var v_queue_cnt_cmd="select count(*) as RCNT from utility.edw_process_batch_ctl where process_id="+v_process_id+" and batch_status=''Q'';";
	var v_queue_cnt_stmt=snowflake.createStatement( {sqlText: v_queue_cnt_cmd} );
	//var queryText = v_queue_cnt_stmt.getSqlText();
	var v_queue_cnt_res=v_queue_cnt_stmt.execute();
	v_queue_cnt_res.next();
	var v_queue_cnt_val=v_queue_cnt_res.getColumnValue(1);
	var finals=5
	//Current Process_id with Running count
    var v_running_cnt_cmd="select count(1) as RCNT from utility.edw_process_batch_ctl where process_id="+v_process_id+" and batch_status=''R'';";
    var v_running_cnt_stmt=snowflake.createStatement( {sqlText: v_running_cnt_cmd} );
    var v_running_cnt_res=v_running_cnt_stmt.execute();
    v_running_cnt_res.next();
    var v_running_cnt_val=v_running_cnt_res.getColumnValue(1);
     
    try {
    if ( ( v_running_cnt_val == 0 ) )
    {  
    	if ( ( v_queue_cnt_val == 0 ) )
 		{
            var v_ins_cmd = "INSERT \\
	                        INTO \\
	                          UTILITY.EDW_PROCESS_BATCH_CTL (BATCH_ID, \\
	                                           PROCESS_ID, \\
	                                           LWM_DTTM, \\
	                                           HWM_DTTM, \\
	                                           BATCH_STATUS, \\
	                                           COMMENTS, \\
	                                           EDW_INS_DTTM, \\
	                                           EDW_UPD_DTTM) \\
                                               (SELECT UTILITY.EDW_PROCESS_BATCH_CTL_SEQ.NEXTVAL,"+ v_process_id + ", \\
                                                (select coalesce(max(hwm_dttm),TO_DATE(''1900/01/01 12:00:00'', ''YYYY/MM/DD HH:MI:SS'')::TIMESTAMP_NTZ ) LWM_DTTM \\
                                                  from utility.edw_process_info info join \\
                                                  utility.edw_process_batch_ctl ctl \\
                                                       on CTL.PROCESS_ID ="+ v_process_id + "\\
                                                    AND CTL.BATCH_STATUS=''S'')  LWM_DTTM,  \\
                                                     CURRENT_TIMESTAMP  HWM_DTTM, \\
                                                       ''Q'', \\
                                                      ''BATCH QUEUED'', \\
                                                      CURRENT_TIMESTAMP  EDW_INS_DTTM, \\
                                                   CURRENT_TIMESTAMP  EDW_INS_DTTM FROM DUAL);";
                  var v_ins_cmd_stmt=snowflake.createStatement( {sqlText: v_ins_cmd} );
                  
                  var v_ins_cmd_exec=v_ins_cmd_stmt.execute();
  		}
      else
      	{
      
      	var upd_cmd="update utility.EDW_PROCESS_BATCH_CTL set batch_status=''D'',EDW_UPD_DTTM=CURRENT_TIMESTAMP,COMMENTS=''BATCH DID NOT RUN'' WHERE PROCESS_ID="+v_process_id+" and batch_status=''Q''";
      	var upd_cmd_stmt=snowflake.createStatement( {sqlText: upd_cmd} );
      	var queryText = upd_cmd_stmt.getSqlText();
      	var upd_cmd_res=upd_cmd_stmt.execute();
      
        var v_ins_cmd = "INSERT \\
	                        INTO \\
	                          UTILITY.EDW_PROCESS_BATCH_CTL (BATCH_ID, \\
	                                           PROCESS_ID, \\
	                                           LWM_DTTM, \\
	                                           HWM_DTTM, \\
	                                           BATCH_STATUS, \\
	                                           COMMENTS, \\
	                                           EDW_INS_DTTM, \\
	                                           EDW_UPD_DTTM) \\
                                               (SELECT UTILITY.EDW_PROCESS_BATCH_CTL_SEQ.NEXTVAL,"+ v_process_id + ", \\
                                                (select coalesce(max(hwm_dttm),TO_DATE(''1900/01/01 12:00:00'', ''YYYY/MM/DD HH:MI:SS'')::TIMESTAMP_NTZ ) LWM_DTTM \\
                                                  from utility.edw_process_info info join \\
                                                  utility.edw_process_batch_ctl ctl \\
                                                       on CTL.PROCESS_ID ="+ v_process_id + "\\
                                                    AND CTL.BATCH_STATUS=''S'')  LWM_DTTM,  \\
                                                     CURRENT_TIMESTAMP  HWM_DTTM, \\
                                                       ''Q'', \\
                                                      ''BATCH QUEUED'', \\
                                                      CURRENT_TIMESTAMP  EDW_INS_DTTM, \\
                                                   CURRENT_TIMESTAMP  EDW_INS_DTTM FROM DUAL);";
                  var v_ins_cmd_stmt=snowflake.createStatement( {sqlText: v_ins_cmd} );
                  
                  var v_ins_cmd_exec=v_ins_cmd_stmt.execute();
      	}
      
    }
    else 
    {
     throw  " process_id "+v_process_id+" is already in running state";
    }
    //closing try block
    }
    catch (err) {
        return "Error: " + err;
    }
    
    //closing while loop
    } 
    
    
return " initiated queue for process_id "+v_process_id;
';