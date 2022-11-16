/* ====================
Output Dataset:
	1. Training & Testing : work.Merge_All_Data
	2. Valid : work.valid
==================== */

proc sort data=work.Merge_All_Data;by alert_key;run;
proc sort data=work.valid;by alert_key;run;

proc sql;
create table valid_data as
select target.alert_key, buf.*
from valid target
left join Merge_All_Data(drop=sar_flag) buf on target.alert_key=buf.alert_key
;
quit;


/* ==================== Gen. CSV ==================== */
/*columns count : ??*/
proc export data=work.Merge_All_Data(where=(missing(sar_flag)=0)) outfile="&path.\EG\TRAIN_ALERT.csv" dbms=csv replace;run;

/*columns count : ??*/
proc export data=work.valid_data outfile="&path.\EG\VALID_ALERT.csv" dbms=csv replace;run;


/* ==================== Gen. Dataset ==================== */
libname out 'C:\Janice\Other\競賽\玉山_2022\Data\訓練資料集_first\EG\Data';
data out.Merge_All_Data;
set Merge_All_Data;
run;
data out.Valid_Data;
set valid_data;
run;