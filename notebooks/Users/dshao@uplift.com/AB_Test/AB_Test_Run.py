# Databricks notebook source
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
import numpy as np
import pandas as pd

# COMMAND ----------

# MAGIC %run /Users/dshao@uplift.com/AB_Test/AB_Test_Functions

# COMMAND ----------

### Set Parameters
start_date = "2023-06-05"
end_date = '2023-06-12'
control_id = 'plaid_only-iframe-ab-testing|a|0.5'
treatment_id = 'plaid_only-iframe-ab-testing|b|0.5'
exp_id = 'plaid_only-iframe-ab-testing'
upcode_1,upcode_2,upcode_3 = 'UP-60387889-1','',''
state_code = 6

#special treatment when Shiva assigned majority to both arms.
#when certain correction on URL is possible.
URL = 'https://booking.flyfrontier.com%'

# if remove returning user who have seen the testing screen within 14 days.
if_1st='general'
# if special treatment is necessary for assigning arms.
if_special='special'


# COMMAND ----------

# MAGIC %run /Users/dshao@uplift.com/AB_Test/AB_Test_Data_Cleaning

# COMMAND ----------

# MAGIC %run /Users/dshao@uplift.com/AB_Test/AB_Test_Visualization_1

# COMMAND ----------

r1 = create_result(tb_agg).withColumn('user segment',lit('all users'))
r2 = create_result(tb_seg_new).withColumn('user segment',lit('new users'))
r3 = create_result(tb_seg_repeat).withColumn('user segment',lit('repeat users'))

output = r1.unionAll(r2).unionAll(r3)
display(output.select('user segment','control_count','control_success','treatment_count','treatment_success',
                      'control_CR','treatment_CR','z_stat','p_value','control CI','treatment CI'))

# COMMAND ----------

### Fico Case
# find quantiles to cut bins. Max 6 bins.
bins1 = df_join.select(percentile_approx('FICO', [0.2, 0.4, 0.6, 0.8], 1000000)).collect()[0][0]
bins1.insert(0,-float('inf'))
bins1.append(float('inf'))
print(bins1)

col_input1 = 'FICO'
col_output1 = 'FICO_BIN'
df1 = df_join
df2 = df_join.filter(col('USER_FLAG_FIRST')=='new user')
user_seg1 = 'all users'
user_seg2 = 'new users'
p = 1

# COMMAND ----------

#sr1 = bucketizer(bins1,col_input1,col_output1,df1,user_seg1, p)
#sr2 = bucketizer(bins1,col_input1,col_output1,df2,user_seg2, p)
#output_strata_fico = sr1[0].unionAll(sr2[0]).select('user segment','control_count','control_success','treatment_count','treatment_success','control_CR','treatment_CR','z_stat','p_value','control CI','treatment CI')
#display(output_strata_fico)

# COMMAND ----------

# Model Score Case
bins2 = df_join.select(percentile_approx('LOSS_RATE_SCORE', [0.2, 0.4, 0.6, 0.8], 1000000)).collect()[0][0]
bins2.insert(0,-float('inf'))
bins2.append(float('inf'))
print(bins2)
col_input2 = 'LOSS_RATE_SCORE'
col_output2 = 'SCORE_BIN'  

# COMMAND ----------

sr3 = bucketizer(bins2,col_input2,col_output2,df1,user_seg1, p)
sr4 = bucketizer(bins2,col_input2,col_output2,df2,user_seg2, p)
output_strata_score = sr3[0].unionAll(sr4[0]).select('user segment','control_count','control_success','treatment_count','treatment_success','control_CR','treatment_CR','z_stat','p_value','control CI','treatment CI')
display(output_strata_score)

# COMMAND ----------

# original data distribution between control and treatment
fico_bin = [-float('inf'),500,550,600,650,700,750,800,850,float('inf')]
order_bin = [-float('inf'),300,500,1100,2000,5000,float('inf')]
score_bin = [-float('inf'),0.2,0.4,0.6,0.8,float('inf')]

# COMMAND ----------

# stratified data distribution between control and treatment
b_fico = Bucketizer(splits = fico_bin,inputCol = 'FICO', outputCol = 'IV_FICO')
iv_fico = b_fico.setHandleInvalid("keep").transform(df_join)
display(iv_fico.groupBy('IV_FICO','GROUP').count())

# COMMAND ----------

b_order = Bucketizer(splits = order_bin,inputCol = 'ORDER_AMOUNT', outputCol = 'IV_ORDER_AMOUNT')
iv_order = b_order.setHandleInvalid("keep").transform(df_join)
display(iv_order.groupBy('IV_ORDER_AMOUNT','GROUP').count())

# COMMAND ----------

b_score = Bucketizer(splits = score_bin,inputCol = 'LOSS_RATE_SCORE', outputCol = 'IV_SCORE')
iv_score = b_score.setHandleInvalid("keep").transform(df_join)
display(iv_score.groupBy('IV_SCORE','GROUP').count())

# COMMAND ----------

b_fico_s = Bucketizer(splits = fico_bin,inputCol = 'FICO', outputCol = 'IV_FICO')
iv_fico_s = b_fico.setHandleInvalid("keep").transform(sr3[1].join(df_join,sr3[1].CID == df_join.CID,"left").drop(df_join.GROUP))
display(iv_fico_s.groupBy('IV_FICO','GROUP').count())

# COMMAND ----------

b_order = Bucketizer(splits = order_bin,inputCol = 'ORDER_AMOUNT', outputCol = 'IV_ORDER_AMOUNT')
iv_order = b_order.setHandleInvalid("keep").transform(sr3[1].join(df_join,sr3[1].CID == df_join.CID,"left").drop(df_join.GROUP))
display(iv_order.groupBy('IV_ORDER_AMOUNT','GROUP').count())

# COMMAND ----------

b_score = Bucketizer(splits = score_bin,inputCol = 'LOSS_RATE_SCORE', outputCol = 'IV_SCORE')
iv_score = b_score.setHandleInvalid("keep").transform(sr3[1].join(df_join,sr3[1].CID == df_join.CID,"left").drop(df_join.GROUP))
display(iv_score.groupBy('IV_SCORE','GROUP').count())

# COMMAND ----------

b_score = Bucketizer(splits = score_bin,inputCol = 'LOSS_RATE_SCORE', outputCol = 'IV_SCORE')
iv_score = b_score.setHandleInvalid("keep").transform(sr3[1].join(df_join,sr3[1].CID == df_join.CID,"left").drop(df_join.GROUP))
display(iv_score.groupBy('IV_SCORE','GROUP').count())