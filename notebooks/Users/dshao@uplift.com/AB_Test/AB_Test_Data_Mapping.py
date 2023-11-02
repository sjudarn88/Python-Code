# Databricks notebook source
# MAGIC %run Shared/get_snowflake_no_install

# COMMAND ----------

sf_conn_parameters = sf_conf("data-analytics")[0]

#Import Packages & Define Functions
from pyspark.sql.functions import *
import pyspark.sql.types as T
from statsmodels.stats.proportion import proportions_ztest, proportion_confint
from pyspark.sql.types import LongType,DoubleType, FloatType, ArrayType,IntegerType
from pyspark.sql.functions import udf
from pyspark.sql.window import Window
from pyspark.ml.feature import Bucketizer
from pyspark.sql import SparkSession

import json

# COMMAND ----------

def parse_array_from_string(x):
	res = json.loads(x)
	return res
  
retrieve_array = udf(parse_array_from_string, T.ArrayType(T.StringType()))

# COMMAND ----------

def sf_sql(qry):
  SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
  df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
               .options(**sf_conn_parameters) \
               .option("query", qry).load()
  return df

# COMMAND ----------

start_date = '2023-09-03'
end_date = '2023-09-04'
control_id = 'CC-EXP|a|0.5'
treatment_id = 'CC-EXP|b|0.5'
exp_id = 'CC-EXP'
upcode_1,upcode_2,upcode_3,upcode_4,upcode_5 = 'UP-74812101-2','UP-98145157-4','UP-93632994-1','UP-13472901-2','UP-55675619-2'
state_code = 6
#special treatment when Shiva assigned majority to both arms.
#when certain correction on URL is possible.
URL = 'https://payment.emirates.com/payment%'

# if remove returning user who have seen the testing screen within 14 days.
if_1st='general'
# if special treatment is necessary for assigning arms.
if_special='special'

# COMMAND ----------

### Standard Cevents
query1 = f"""
  select cid,upcode,device_type,browser,date,f.state_code,s.state,experiments 
      from "PRODUCTION_DB"."PUBLIC"."PAY_MONTHLY_FUNNEL_FLAGGED" f 
      left join "PRODUCTION_DB"."PUBLIC"."PAY_MONTHLY_FUNNEL_STATES" s 
        on f.state_code=s.state_code 
      where is_good
        and date >= '{start_date}' and date < '{end_date}'
        and upcode in ('{upcode_1}','{upcode_2}','{upcode_3}','{upcode_4}','{upcode_5}')
        and f.state_code = '{state_code}'
        and (experiments::string ilike concat('%','{control_id}','%') or experiments::string ilike concat('%','{treatment_id}','%'))
        --and (array_contains('{control_id}'::variant,experiments) or array_contains('{treatment_id}'::variant,experiments))
        and upcode!=''
          """

# COMMAND ----------

### Return User Data
# Use 14 days before the test launch and only consider users who arrived at the changed step.
# (if the user is a return user but didn't arrive at the changed step, then considered 1st-time users for the change)
query2 = f"""
  select cid
      from "PRODUCTION_DB"."PUBLIC"."PAY_MONTHLY_FUNNEL_FLAGGED" f 
      left join "PRODUCTION_DB"."PUBLIC"."PAY_MONTHLY_FUNNEL_STATES" s 
        on f.state_code=s.state_code 
      where is_good
        and date >= dateadd('day',-2,'{start_date}') and date < dateadd('day',-1,'{start_date}')
        and upcode in ('{upcode_1}','{upcode_2}','{upcode_3}','{upcode_4}','{upcode_5}')
        and f.state_code = '{state_code}'
        and upcode != ''
      group by 1
          """

# COMMAND ----------

### Special Case: CID presented to both arms due to multiple URL domain.
query3 = f"""
with t as(
      select cid,upcode,datetime
        ,replace(replace(try_parse_json(campaign_keyword):"experiments",'[',''),']','') as exp
      from "PRODUCTION_DB"."PUBLIC"."CLICKSTREAM_RAW"
      where date >= dateadd('day',-2,'{start_date}') and date < '{end_date}'
        and upcode in ('{upcode_5}')
        and url ilike '{URL}'
        and upcode != ''
),ext as ( 
 select cid,upcode,datetime,replace(try_parse_json(exp):"id",'"','') as id
        ,replace(try_parse_json(exp):"g",'"','') as g
        ,try_parse_json(exp):"w" as w
   from t
   where replace(try_parse_json(exp):"id",'"','') ilike concat('%','{exp_id}','%')
    and replace(try_parse_json(exp):"g",'"','') in ('a','b')
) select cid as cid_3
        ,upcode as upcode_3
        ,concat(id,'|',g,'|',w) experiments_3
        ,g
        ,row_number() over (partition by cid,upcode order by datetime) as rank
     from ext
     qualify rank=1
          """

# COMMAND ----------

df_raw = sf_sql(query1)
df_return = sf_sql(query2).withColumnRenamed('CID','CID_2')

# COMMAND ----------

df_raw.cache()
df_return.cache()

# COMMAND ----------

df_raw.count()

# COMMAND ----------

df_return.cache()

# COMMAND ----------

df_return.count()

# COMMAND ----------

        #.withColumn('A',when(array_contains(col('EXPERIMENT'),control_id),lit('1')).otherwise(lit('0')))\
        #.withColumn('B',when(array_contains(col('EXPERIMENT'),treatment_id),lit('1')).otherwise(lit('0')))\

# COMMAND ----------

df_raw = df_raw.withColumn('EXPERIMENT', retrieve_array(col('EXPERIMENTS')))\
        .withColumn('DATE',col('DATE').cast('date'))\
        .withColumn('A',when(col('EXPERIMENT').cast('string').contains(control_id),lit('1')).otherwise(lit('0'))) \
        .withColumn('B',when(col('EXPERIMENT').cast('string').contains(treatment_id),lit('1')).otherwise(lit('0'))) \
        .withColumn('Group',when((col('A')==0) & (col('B')==1),'treatment')\
                         .when((col('A')==1) & (col('B')==0),'control')\
                         .when((col('A')==1) & (col('B')==1),'both')\
                        .otherwise('exclude'))\
        .drop('EXPERIMENTS')

# COMMAND ----------

df_raw.count()

# COMMAND ----------

# first-time user for the version 
df_1st = df_raw.join(df_return,df_raw.CID == df_return.CID_2,'left').filter(col('CID_2').isNull()).drop('CID_2')

# COMMAND ----------

df_1st.cache()

# COMMAND ----------

df_1st.count()

# COMMAND ----------

# Check Percentage of CID on both arms
print('\nTotal Counts of CIDs:'
      ,df_raw.select(countDistinct('CID')).collect()[0][0]
      ,'\nPercentage of CID on both arms:'
      ,df_raw.filter(col('GROUP')=='both').select(countDistinct('CID')).collect()[0][0]/df_raw.select(countDistinct('CID')).collect()[0][0]
      ,'\nPercent of first-time users:'
      ,df_1st.select(countDistinct('CID')).collect()[0][0]/df_raw.select(countDistinct('CID')).collect()[0][0]
     )

# COMMAND ----------

def cevent_choose(if_1st,if_special):
  if if_special == 'general':
    if if_1st == 'general':
      return df_raw
    else:
      return df_1st
  else:
    df_special = sf_sql(query3)
    df_raw_s = df_raw.join(df_special,(df_raw.CID == df_special.CID_3) & (df_raw.UPCODE == df_special.UPCODE_3),'left')
    # change arms due to specific sub-domains 
    df_raw_s = df_raw_s.withColumn('G',when(col('G')=='a','control')\
                                      .when(col('G')=='b','treatment')\
                                      .otherwise('both'))\
                        .withColumn('Group',when((col('Group')=='both') & (col('UPCODE')==upcode_5),col('G'))\
                                      .otherwise(col('Group')))\
                                      .drop('CID_3','UPCODE_3','G','RANK')
    
    if if_1st == 'general':
      return df_raw_s
    else:
      df_1st_s = df_raw_s.join(df_return,df_raw.CID == df_return.CID_2,'left').filter(col('CID_2').isNull()).drop('CID_2')
      return df_1st_s

# COMMAND ----------

df = cevent_choose(if_1st,if_special)

# COMMAND ----------

df.cache()
df.count()

# COMMAND ----------

#Check if CID with both arms are treated.'if_special'
#print('\nTotal Counts of records:'
#      ,df.select(countDistinct('CID')).collect()[0][0]
#      ,'\nPercentage of CID on both arms:'
#      ,df.filter(col('GROUP')=='both').select(countDistinct('CID')).collect()[0][0]/df.select(countDistinct('CID')).collect()[0][0]
#)

# COMMAND ----------

windowSpec = Window.partitionBy('CID').orderBy('DATE')
df_group = df.filter((col('STATE_CODE') == state_code) & (col('Group').isin('control','treatment')))\
              .withColumn('RN',row_number().over(windowSpec))
df_group = df_group.filter(col('RN')==1).select(col('CID'),col('UPCODE'),col('Group'),col('DEVICE_TYPE'))

# COMMAND ----------

df_group.cache()
df_group.count()

# COMMAND ----------

#df_group.select(countDistinct('CID')).collect()[0][0]

# COMMAND ----------

# DBTITLE 1,Biz-Event Mapping
query_b = f"""
with t as (
  select correlation_id,cid,upcode,flow_id,user_flag,application_id
      ,mantle_decision,step_ac,timestamp_mc,timestamp_ac
      ,row_number() over (partition by cid order by timestamp_mc) as rank 
    from hive_metastore.analytics.return_user_stg_flow
    where timestamp_mc >= '{start_date}' and timestamp_mc < '{end_date}' and mantle_decision='approved'
      and flow_id ilike '%return%'
      and upcode in ('{upcode_1}','{upcode_2}','{upcode_3}','{upcode_4}','{upcode_5}')
      and application_id is not null
      and upcode != ''
)
, t2 as (
  select id,fico,final_grade
    from kasa.int_ficoresponses
)
, t3 as (
  select correlation_id,application_id,generated_features_uws_a10_001_fico as fico,final_grade,model_score,model_used
    from bizevent_tier1.int_uws_underwrite_only
)
, t4 as (
  select payload_correlation_id as correlation_id,payload_flow_id as flow_id,payload_order_amount as order_amount
    from bizevent_tier1.stg_application_flow_order
)
, t5 as (
  select applicationid
    from bizevent_tier1.int_fds_transaction_authorized 
    where lower(response) = 'approval'
    group by 1
)
, t6 as (
  select applicationid
    from bizevent_tier1.int_fds_transaction_settled 
    where lower(response) = 'approval' and transactiontype = 'D'
    group by 1
)
, t7 as (
  select payload_account_id as account_id,payload_correlation_id as correlation_id, payload_flow_id as flow_id
    from bizevent_tier1.stg_application_flow_account_identification
)
select t.*,order_amount,account_id,model_used
    ,case when t5.applicationid is null then 0 else 1 end as disburse_auth
    ,case when t6.applicationid is null then 0 else 1 end as disburse_settle
    ,fico,final_grade,model_score from t 
    left join t3 on t.application_id=t3.application_id
    left join t4 on t.correlation_id=t4.correlation_id and t.flow_id=t4.flow_id
    left join t5 on t.application_id=t5.applicationid
    left join t6 on t.application_id=t6.applicationid
    left join t7 on t.correlation_id=t7.correlation_id and t.flow_id=t7.flow_id
"""

# COMMAND ----------

df_uf = spark.sql(query_b)

# COMMAND ----------

df_uf.cache()
df_uf.count()

# COMMAND ----------

#df_uf.select(countDistinct('CID')).collect()[0][0]

# COMMAND ----------

# create dataset with user segmentation
windowSpec2 = Window.partitionBy('CID').orderBy('timestamp_mc')

df_uf =df_uf.withColumn('DATE',to_date(col('timestamp_mc')))\
            .withColumn('timestamp_mc',unix_timestamp(to_timestamp(col('timestamp_mc'))))\
            .withColumn('timestamp_ac',unix_timestamp(to_timestamp(col('timestamp_ac'))))\
            .withColumn('DiffInSeconds',col('timestamp_ac')-col('timestamp_mc'))\
            .withColumn('order_amount',round(col('order_amount')/100,2).cast('FLOAT'))\
            .withColumn('IF_APPROVE',when(col('MANTLE_DECISION').isNull(),lit('0')).otherwise(lit('1')))\
            .withColumn('IF_ACCEPT',when(col('STEP_AC').isNull(),lit('0')).otherwise(lit('1')))\
            .withColumn('USER_FLAG',split(col('USER_FLAG'),'-').getItem(0))\
            .withColumn('USER_FLAG_FIRST',first('USER_FLAG').over(windowSpec2))\
            .withColumn('fico',col('fico').cast('FLOAT'))\
            .withColumn('model_score',col('model_score').cast('FLOAT'))\
            .withColumn('loss_rate_score',loss_odds(col('model_used'),col('model_score')))

# COMMAND ----------

#Merge Cevent with Biz-event to add user flag on CID level
df_join=df_group.join(df_uf,(df_group.CID==df_uf.cid) & (df_group.UPCODE==df_uf.upcode),"inner").drop(df_uf.cid,df_uf.upcode)

# COMMAND ----------

#print('\nTotal Unique CIDs after joining biz-event:'
#      ,df_join.select(countDistinct('CID')).collect()[0][0]
#      ,'\n% Loss after joining biz-event:'
#      ,1-(df_join.select(countDistinct('CID')).collect()[0][0])/(df_uf.select(countDistinct('CID')).collect()[0][0])
#)

# COMMAND ----------

new_column_name_list= list(map(lambda x: x.upper(), df_join.columns))
df_join = df_join.toDF(*new_column_name_list)

df_dup_acct = df_join.groupBy('ACCOUNT_ID').agg(countDistinct('CID').alias('DUP_ACID')).filter((col('DUP_ACID')>1) & col('ACCOUNT_ID').isNotNull())
#df_join.printSchema()
#df_dup_acct.show()
#df_uf.filter(col('ACCOUNT_ID')=='G21315162572').show()

# COMMAND ----------

# Check Percentage of of Account_id has 2+ CIDs
#print('\nTotal Counts of Account:'
#      ,df_join.select(countDistinct('ACCOUNT_ID')).collect()[0][0]
#      ,'\nPercentage of Account_id has 2+ CIDs:'
#      ,df_dup_acct.count()/df_join.select(countDistinct('CID')).collect()[0][0]
#     )

# COMMAND ----------

df_join = df_join.join(df_dup_acct,df_join.ACCOUNT_ID == df_dup_acct.ACCOUNT_ID,"left").filter(col('DUP_ACID').isNull()).drop('DUP_ACID') 

# COMMAND ----------

df_join.cache()

# COMMAND ----------

#df_join.printSchema()

# COMMAND ----------

#output = df_join.select(col('APPLICATION_ID'),col('GROUP'))

# COMMAND ----------

#adhoc
#output.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("analytics.ds_temp_abtest_ccremoval")

# COMMAND ----------

