/* ====================
Result:
	1. Basical : 
	2. Extension : 
	3. ALL : Merge_All_Data
==================== */


/* ==================== Basical ==================== */
proc sql;/*obs=25751*/
create table work.Train_alert_data as
select cust.cust_id, cust.alert_key
	 , cust.risk_rank, cust.occupation_code, cust.total_asset, cust.AGE
	 , target.sar_flag
	 , case when missing(target.alert_key)=0 then alert_date.date else valid_alert.date end as alert_date
  from work.cust
  left join work.target 	  on target.alert_key = cust.alert_key
  left join work.alert_date  on target.alert_key = alert_date.alert_key
  left join work.valid_alert  on cust.alert_key = valid_alert.alert_key
;
quit;

proc sort data=work.ccba;by cust_id descending byymm ;run;
data work.ccba;
set work.ccba;
by cust_id;
last_byymm = lag(byymm)-1;
if first.cust_id then last_byymm=394;
run;
proc sort data=work.ccba;by cust_id byymm;run;

proc sql;
create table ccba_tmp as 
select distinct target.sar_flag, target.alert_key, target.alert_date, target.cust_id
	 , ccba.lupay, ccba.byymm, ccba.cycam, ccba.usgam, ccba.clamt, ccba.csamt, ccba.inamt, ccba.cucsm, ccba.cucah
  from work.Train_alert_data target
  join work.ccba 	on target.cust_id = ccba.cust_id and target.alert_date between byymm and last_byymm
 group by target.sar_flag, target.alert_key, target.cust_id
;
quit;


proc sql;
/*obs=745184*/
create table cdtx_tmp as
select distinct target.sar_flag, target.alert_key, target.cust_id
	 , sum(cdtx.amt) as total_amt
	 , count(distinct cdtx.country) as country_cnt
	 , count(distinct cdtx.cur_type) as cur_type_cnt
  from work.Train_alert_data target
  join work.cdtx 	on target.cust_id = cdtx.cust_id and cdtx.date <= target.alert_date
 group by target.sar_flag, target.alert_key, target.cust_id
;
quit;

%macro loop_txn_type();
	%global asset_type TxID_type;
	proc sql;
	select distinct info_asset_code /*, count(1) as cnt*/
	into :asset_type separated by ','
	from work.dp
	;
	select distinct fiscTxId /*, count(1) as cnt*/
	into :TxID_type separated by ','
	from work.dp
	;
	quit;
	%put &=asset_type &=TxID_type;


	proc sql;
	/*obs=1801664*/
	create table dp_tmp as
	select distinct target.sar_flag, target.alert_key, target.cust_id
		 , sum(dp.tx_amt*dp.exchg_rate) as total_txn_amt
		 , sum(case when dp.debit_credit='CR' then dp.tx_amt*dp.exchg_rate end) as total_cr_amt
		 , sum(case when dp.debit_credit='DB' then dp.tx_amt*dp.exchg_rate end) as total_dr_amt
		 , sum(case when dp.tx_type = 1 and dp.info_asset_code = 12 then 1 else 0 end) as onboarding_txn_cnt
		 , sum(case when dp.debit_credit='CR' and dp.tx_type = 1 and dp.info_asset_code = 12 then 1 else 0 end) as onboarding_cr_cnt
		 , sum(case when dp.debit_credit='DB' and dp.tx_type = 1 and dp.info_asset_code = 12 then 1 else 0 end) as onboarding_dr_cnt
		 , sum(dp.cross_bank) as cross_txn_cnt
		 , sum(case when dp.debit_credit='CR' and dp.cross_bank=1 then 1 else 0 end) as cross_cr_cnt
		 , sum(case when dp.debit_credit='DB' and dp.cross_bank=1 then 1 else 0 end) as cross_dr_cnt
		 , sum(dp.ATM) as ATM_txn_cnt
		 , sum(case when dp.debit_credit='CR' and dp.ATM=1 then 1 else 0 end) as ATM_cr_cnt
		 , sum(case when dp.debit_credit='DB' and dp.ATM=1 then 1 else 0 end) as ATM_dr_cnt
		 /*20221031 Added Start*/
		 , sum(case when dp.debit_credit='CR' and dp.cross_bank=1 then dp.tx_amt*dp.exchg_rate end) as cross_cr_amt
		 , sum(case when dp.debit_credit='DB' and dp.cross_bank=1 then dp.tx_amt*dp.exchg_rate end) as cross_dr_amt
		 , sum(case when dp.debit_credit='CR' and dp.ATM=1 then dp.tx_amt*dp.exchg_rate end) as ATM_cr_amt
		 , sum(case when dp.debit_credit='DB' and dp.ATM=1 then dp.tx_amt*dp.exchg_rate end) as ATM_dr_amt
		 /*20221031 Added End*/
		 /*20221101 Added Start*/
		 , sum(case when dp.cross_bank=1 then dp.tx_amt*dp.exchg_rate end) as cross_txn_amt
		 , sum(case when dp.ATM=1 then dp.tx_amt*dp.exchg_rate end) as ATM_txn_amt
		 /*20221101 Added End*/
		 /*20221113 Added Start*/
		 , sum(case when dp.tx_type = 1 and dp.info_asset_code = 12 then dp.tx_amt*dp.exchg_rate end) as onboarding_txn_amt
		 , sum(case when dp.debit_credit='CR' and dp.tx_type = 1 and dp.info_asset_code = 12 then dp.tx_amt*dp.exchg_rate end) as onboarding_cr_amt
		 , sum(case when dp.debit_credit='DB' and dp.tx_type = 1 and dp.info_asset_code = 12 then dp.tx_amt*dp.exchg_rate end) as onboarding_dr_amt
		 %do i = 1 %to %eval(%sysfunc(count(%quote(&asset_type.), %quote(,)))+1);
			 %let the_asset = %scan(%quote(&asset_type.), &i, %quote(,));
			 , sum(case when dp.info_asset_code = &the_asset. then 1 else 0 end) as asset_txn_cnt&the_asset.
			 , sum(case when dp.debit_credit='CR' and dp.info_asset_code = &the_asset. then 1 else 0 end) as asset_cr_cnt&the_asset.
			 , sum(case when dp.debit_credit='DB' and dp.info_asset_code = &the_asset. then 1 else 0 end) as asset_dr_cnt&the_asset.
			 , sum(case when dp.info_asset_code = &the_asset. then dp.tx_amt*dp.exchg_rate end) as asset_txn_amt&the_asset.
			 , sum(case when dp.debit_credit='CR' and dp.info_asset_code = &the_asset. then dp.tx_amt*dp.exchg_rate end) as asset_cr_amt&the_asset.
			 , sum(case when dp.debit_credit='DB' and dp.info_asset_code = &the_asset. then dp.tx_amt*dp.exchg_rate end) as asset_dr_amt&the_asset.
		 %end;
		 %do i = 1 %to %eval(%sysfunc(count(%quote(&TxID_type.), %quote(,)))+1);
			 %let the_TxID = %scan(%quote(&TxID_type.), &i, %quote(,));
			 , sum(case when dp.fiscTxId = "&the_TxID." then 1 else 0 end) as TxID_txn_cnt&the_TxID.
			 , sum(case when dp.debit_credit='CR' and dp.fiscTxId = "&the_TxID." then 1 else 0 end) as TxID_cr_cnt&the_TxID.
			 , sum(case when dp.debit_credit='DB' and dp.fiscTxId = "&the_TxID." then 1 else 0 end) as TxID_dr_cnt&the_TxID.
			 , sum(case when dp.fiscTxId = "&the_TxID." then dp.tx_amt*dp.exchg_rate end) as TxID_txn_amt&the_TxID.
			 , sum(case when dp.debit_credit='CR' and dp.fiscTxId = "&the_TxID." then dp.tx_amt*dp.exchg_rate end) as TxID_cr_amt&the_TxID.
			 , sum(case when dp.debit_credit='DB' and dp.fiscTxId = "&the_TxID." then dp.tx_amt*dp.exchg_rate end) as TxID_dr_amt&the_TxID.
		 %end;
		 /*20221113 Added End*/
	  from work.Train_alert_data target
	  join work.dp 	on target.cust_id = dp.cust_id and dp.tx_date <= target.alert_date
	 group by target.sar_flag, target.alert_key, target.cust_id
	;
	quit;
%mend;
%loop_txn_type;

proc sql;
create table remi_tmp as 
select distinct target.sar_flag, target.alert_key, target.cust_id, sum(trade_amount_usd) as total_trade_amt
  from work.Train_alert_data target
  join work.remi 	on target.cust_id = remi.cust_id and remi.trans_date <= target.alert_date
 group by target.sar_flag, target.alert_key, target.cust_id
;
quit;


/* ==================== Extension ==================== */
/*客戶過往申報 SAR 紀錄*/
proc sql;
create table work.before_alert_sar_tmp as
select distinct train.cust_id, train.alert_key , train.sar_flag, train.alert_date
	 , cust.alert_key as other_alt_key
	 , target.sar_flag as other_alt_sar
	 , case when missing(target.alert_key)=0 then alert_date.date else valid_alert.date end as other_alt_date
  from work.Train_alert_data train
  left join work.cust 		  on train.cust_id = cust.cust_id and train.alert_key <> cust.alert_key
  left join work.target 	  on cust.alert_key = target.alert_key 
  left join work.alert_date   on cust.alert_key = alert_date.alert_key
  left join work.valid_alert  on cust.alert_key = valid_alert.alert_key
where alert_date.date <= train.alert_date or valid_alert.date <= train.alert_date
;

create table work.before_alert_sar as
select distinct cust_id, alert_key
	 , sum(case when missing(other_alt_key)=0 then 1 else 0 end) as before_alt_times
	 , sum(other_alt_sar) as before_sar_times
	 , min(alert_date - other_alt_date) as min_last_alt_dt_diff
from work.before_alert_sar_tmp
group by cust_id, alert_key
;
quit;


/*客戶整年度交易資料*/
%macro loop_avg_txn();
	%put Exist asset_type : %symexist(asset_type);
	%put Exist TxID_type : %symexist(TxID_type);

	proc sql;
	%if not %symexist(asset_type) %then %do;
	select distinct info_asset_code /*, count(1) as cnt*/
	into :asset_type separated by ','
	from work.dp
	;
	%end;
	%if not %symexist(TxID_type) %then %do;
	select distinct fiscTxId /*, count(1) as cnt*/
	into :TxID_type separated by ','
	from work.dp
	;
	%end;
	quit;
	%put &=asset_type &=TxID_type;

	proc sql;
	create table work.cust_txn_profile_tmp1 as
	select distinct target.cust_id, dp.tx_date
		 , sum(tx_amt*dp.exchg_rate) as total_amt
		 , sum(case when dp.debit_credit='CR' then dp.tx_amt*dp.exchg_rate end) as total_cr_amt
		 , sum(case when dp.debit_credit='DB' then dp.tx_amt*dp.exchg_rate end) as total_dr_amt
		 , sum(case when dp.cross_bank=1 then dp.tx_amt*dp.exchg_rate end) as cross_txn_amt
		 , sum(case when dp.debit_credit='CR' and dp.cross_bank=1 then dp.tx_amt*dp.exchg_rate end) as cross_cr_amt
		 , sum(case when dp.debit_credit='DB' and dp.cross_bank=1 then dp.tx_amt*dp.exchg_rate end) as cross_dr_amt
		 , sum(case when dp.ATM=1 then dp.tx_amt*dp.exchg_rate end) as ATM_txn_amt
		 , sum(case when dp.debit_credit='CR' and dp.ATM=1 then dp.tx_amt*dp.exchg_rate end) as ATM_cr_amt
		 , sum(case when dp.debit_credit='DB' and dp.ATM=1 then dp.tx_amt*dp.exchg_rate end) as ATM_dr_amt
		 , sum(case when dp.tx_type = 1 and dp.info_asset_code = 12 then dp.tx_amt*dp.exchg_rate end) as onboarding_txn_amt
		 , sum(case when dp.debit_credit='CR' and dp.tx_type = 1 and dp.info_asset_code = 12 then dp.tx_amt*dp.exchg_rate end) as onboarding_cr_amt
		 , sum(case when dp.debit_credit='DB' and dp.tx_type = 1 and dp.info_asset_code = 12 then dp.tx_amt*dp.exchg_rate end) as onboarding_dr_amt
		 %do i = 1 %to %eval(%sysfunc(count(%quote(&asset_type.), %quote(,)))+1);
		 	%let the_asset = %scan(%quote(&asset_type.), &i, %quote(,));
			 , sum(case when dp.info_asset_code = &the_asset. then dp.tx_amt*dp.exchg_rate end) as asset_txn_amt&the_asset.
			 , sum(case when dp.debit_credit='CR' and dp.info_asset_code = &the_asset. then dp.tx_amt*dp.exchg_rate end) as asset_cr_amt&the_asset.
			 , sum(case when dp.debit_credit='DB' and dp.info_asset_code = &the_asset. then dp.tx_amt*dp.exchg_rate end) as asset_dr_amt&the_asset.
		 %end;
		 %do i = 1 %to %eval(%sysfunc(count(%quote(&TxID_type.), %quote(,)))+1);
		 	%let the_TxID = %scan(%quote(&TxID_type.), &i, %quote(,));
			 , sum(case when dp.fiscTxId = "&the_TxID." then dp.tx_amt*dp.exchg_rate end) as TxID_txn_amt&the_TxID.
			 , sum(case when dp.debit_credit='CR' and dp.fiscTxId = "&the_TxID." then dp.tx_amt*dp.exchg_rate end) as TxID_cr_amt&the_TxID.
			 , sum(case when dp.debit_credit='DB' and dp.fiscTxId = "&the_TxID." then dp.tx_amt*dp.exchg_rate end) as TxID_dr_amt&the_TxID.
		 %end;
	  from work.Train_alert_data target
	  join work.dp 	on target.cust_id = dp.cust_id and dp.tx_date <= target.alert_date
	 group by target.cust_id, dp.tx_date
	;

	create table work.cust_txn_profile as
	select distinct target.cust_id
		 , avg(total_amt) as avg_total_amt
		 , avg(total_cr_amt) as avg_total_cr_amt
		 , avg(total_dr_amt) as avg_total_dr_amt
		 , avg(cross_txn_amt) as avg_cross_amt
		 , avg(cross_cr_amt) as avg_cross_cr_amt
		 , avg(cross_dr_amt) as avg_cross_dr_amt
		 , avg(ATM_txn_amt) as avg_ATM_amt
		 , avg(ATM_cr_amt) as avg_ATM_cr_amt
		 , avg(ATM_dr_amt) as avg_ATM_dr_amt
		 , avg(onboarding_txn_amt) as avg_onboarding_amt
		 , avg(onboarding_cr_amt) as avg_onboarding_cr_amt
		 , avg(onboarding_dr_amt) as avg_onboarding_dr_amt
		 %do i = 1 %to %eval(%sysfunc(count(%quote(&asset_type.), %quote(,)))+1);
		 	%let the_asset = %scan(%quote(&asset_type.), &i, %quote(,));
		 	, avg(asset_txn_amt&the_asset.) as avg_asset_amt&the_asset.
		 	, avg(asset_cr_amt&the_asset.) as avg_asset_cr_amt&the_asset.
		 	, avg(asset_dr_amt&the_asset.) as avg_asset_dr_amt&the_asset.
		 %end;
		 %do i = 1 %to %eval(%sysfunc(count(%quote(&TxID_type.), %quote(,)))+1);
		 	%let the_TxID = %scan(%quote(&TxID_type.), &i, %quote(,));
			 , avg(TxID_txn_amt&the_TxID.) as avg_TxID_amt&the_TxID.
			 , avg(TxID_cr_amt&the_TxID.) as avg_TxID_cr_amt&the_TxID.
			 , avg(TxID_dr_amt&the_TxID.) as avg_TxID_dr_amt&the_TxID.
		 %end;

		 , std(total_amt) as std_total_amt
		 , std(total_cr_amt) as std_total_cr_amt
		 , std(total_dr_amt) as std_total_dr_amt
		 , std(cross_txn_amt) as std_cross_amt
		 , std(cross_cr_amt) as std_cross_cr_amt
		 , std(cross_dr_amt) as std_cross_dr_amt
		 , std(ATM_txn_amt) as std_ATM_amt
		 , std(ATM_cr_amt) as std_ATM_cr_amt
		 , std(ATM_dr_amt) as std_ATM_dr_amt
		 , std(onboarding_txn_amt) as std_onboarding_amt
		 , std(onboarding_cr_amt) as std_onboarding_cr_amt
		 , std(onboarding_dr_amt) as std_onboarding_dr_amt
		 %do i = 1 %to %eval(%sysfunc(count(%quote(&asset_type.), %quote(,)))+1);
		 	%let the_asset = %scan(%quote(&asset_type.), &i, %quote(,));
		 	, std(asset_txn_amt&the_asset.) as std_asset_amt&the_asset.
		 	, std(asset_cr_amt&the_asset.) as std_asset_cr_amt&the_asset.
		 	, std(asset_dr_amt&the_asset.) as std_asset_dr_amt&the_asset.
		 %end;
		 %do i = 1 %to %eval(%sysfunc(count(%quote(&TxID_type.), %quote(,)))+1);
		 	%let the_TxID = %scan(%quote(&TxID_type.), &i, %quote(,));
			 , std(TxID_txn_amt&the_TxID.) as std_TxID_amt&the_TxID.
			 , std(TxID_cr_amt&the_TxID.) as std_TxID_cr_amt&the_TxID.
			 , std(TxID_dr_amt&the_TxID.) as std_TxID_dr_amt&the_TxID.
		 %end;
	  from work.cust_txn_profile_tmp1 target
	 group by target.cust_id
	;
	quit;
%mend;
%loop_avg_txn;


/*客戶外匯交易資訊*/
proc sql;
create table remi_extension as 
select distinct target.sar_flag, target.alert_key, target.cust_id
	 , sum(trade_amount_usd*30) as total_trade_TWD_amt
	 , count(1) as total_txn_cnt
  from work.Train_alert_data target
  join work.remi 	on target.cust_id = remi.cust_id and remi.trans_date <= target.alert_date
 group by target.sar_flag, target.alert_key, target.cust_id
;
quit;



/* ==================== ALL ==================== */

%macro loop_all();
	%put Exist asset_type : %symexist(asset_type);
	%put Exist TxID_type : %symexist(TxID_type);

	proc sql;
	%if not %symexist(asset_type) %then %do;
	select distinct info_asset_code /*, count(1) as cnt*/
	into :asset_type separated by ','
	from work.dp
	;
	%end;
	%if not %symexist(TxID_type) %then %do;
	select distinct fiscTxId /*, count(1) as cnt*/
	into :TxID_type separated by ','
	from work.dp
	;
	%end;
	quit;
	%put &=asset_type &=TxID_type;


	proc sql;
	create table Merge_All_Data as
	select distinct target.sar_flag, target.alert_key, target.alert_date
		 , target.risk_rank, target.occupation_code, target.total_asset, target.AGE
		 , ccba.lupay, ccba.byymm, ccba.cycam, ccba.usgam, ccba.clamt, ccba.csamt, ccba.inamt, ccba.cucsm, ccba.cucah
		 , cdtx.total_amt, cdtx.country_cnt, cdtx.cur_type_cnt
		 , dp.total_txn_amt, dp.total_cr_amt, dp.total_dr_amt, dp.onboarding_txn_cnt, dp.onboarding_cr_cnt, dp.onboarding_dr_cnt
		 , dp.cross_txn_cnt, dp.cross_cr_cnt, dp.cross_dr_cnt, dp.ATM_txn_cnt, dp.ATM_cr_cnt, dp.ATM_dr_cnt
		 , remi.total_trade_amt

		 , dp.cross_cr_amt, dp.cross_dr_amt, dp.ATM_cr_amt, dp.ATM_dr_amt
		 , dp.cross_txn_amt, dp.ATM_txn_amt

		 , last_alt.before_alt_times, last_alt.before_sar_times, last_alt.min_last_alt_dt_diff
		 
		 , avg_total_amt
		 , avg_total_cr_amt
		 , avg_total_dr_amt
		 , avg_cross_amt
		 , avg_cross_cr_amt
		 , avg_cross_dr_amt
		 , avg_ATM_amt
		 , avg_ATM_cr_amt
		 , avg_ATM_dr_amt
		 , avg_onboarding_amt
		 , avg_onboarding_cr_amt
		 , avg_onboarding_dr_amt

		 , std_total_amt
		 , std_total_cr_amt
		 , std_total_dr_amt
		 , std_cross_amt
		 , std_cross_cr_amt
		 , std_cross_dr_amt
		 , std_ATM_amt
		 , std_ATM_cr_amt
		 , std_ATM_dr_amt
		 , std_onboarding_amt
		 , std_onboarding_cr_amt
		 , std_onboarding_dr_amt
		 %do i = 1 %to %eval(%sysfunc(count(%quote(&asset_type.), %quote(,)))+1);
		 %let the_asset = %scan(%quote(&asset_type.), &i, %quote(,));
		 , asset_txn_cnt&the_asset.
		 , asset_cr_cnt&the_asset.
		 , asset_dr_cnt&the_asset.
		 , asset_txn_amt&the_asset.
		 , asset_cr_amt&the_asset.
		 , asset_dr_amt&the_asset.
		 %end;
		 %do i = 1 %to %eval(%sysfunc(count(%quote(&TxID_type.), %quote(,)))+1);
		 %let the_TxID = %scan(%quote(&TxID_type.), &i, %quote(,));
		 , TxID_txn_cnt&the_TxID.
		 , TxID_cr_cnt&the_TxID.
		 , TxID_dr_cnt&the_TxID.
		 , TxID_txn_amt&the_TxID.
		 , TxID_cr_amt&the_TxID.
		 , TxID_dr_amt&the_TxID.
		 %end;
	  from work.Train_alert_data target
	  left join work.ccba_tmp as ccba	on target.alert_key = CCBA.alert_key
	  left join work.cdtx_tmp as cdtx 	on target.alert_key = cdtx.alert_key
	  left join work.dp_tmp as dp   	on target.alert_key = dp.alert_key
	  left join work.remi_tmp as remi	on target.alert_key = remi.alert_key
	  left join work.before_alert_sar as last_alt	on target.alert_key = last_alt.alert_key
	  left join work.cust_txn_profile as prof on target.cust_id = prof.cust_id
	order by target.alert_key
	;
	quit;

%mend;
%loop_all;

