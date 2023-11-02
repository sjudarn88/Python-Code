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

# COMMAND ----------

### Z-test
def parse_array_from_string(x):
	res = json.loads(x)
	return res
  
retrieve_array = udf(parse_array_from_string, T.ArrayType(T.StringType()))

# Define statistics
def ztest(control_count, treatment_count, control_success, treatment_success,tail='larger',threshold=0.05,center=0):
  """
    tail: "two-sided" is for double-tailed test. "larger" is for right-tailed test. "smaller is for left-tailed test"
    threshold: alpha, ususally 0.01,0.05,0.1.azure
    center: 0 if comparing mean of the sample, a number is how far away is added/substracted from mean of the sample. 
  """
  success = [control_success, treatment_success]
  nr_obs = [control_count, treatment_count]
  #pvalue
  z_stat,pvalue = proportions_ztest(count=success,nobs=nr_obs,value=center,alternative=tail)
  #confidence interval
  (lower_control,lower_treatment),(upper_control,upper_treatment) = proportion_confint(success,nobs=nr_obs,alpha=threshold)
  return (float(z_stat),float(pvalue),float(lower_control),float(lower_treatment),float(upper_control),float(upper_treatment))

# COMMAND ----------

### Output Table for Ztest
def create_result(tb):
  ptb = tb.filter(col('GROUP')=='control').alias('c').crossJoin(tb.filter(col('GROUP')=='treatment').alias('t'))\
            .select(col("c.COUNT").alias("control_count"),col("t.COUNT").alias("treatment_count"), 
              col("c.SUCCESS").alias("control_success"),col("t.SUCCESS").alias("treatment_success"))
  
  ZtestUDF = udf(ztest,ArrayType(DoubleType()))
  result_df = ptb.withColumn('ztest_udf',ZtestUDF('control_count','treatment_count','control_success','treatment_success'))
  result_df = result_df.withColumn('control_CR',round(col("control_success")/col("control_count"),4))\
                     .withColumn('treatment_CR',round(col("treatment_success")/col("treatment_count"),4))\
                     .withColumn('z_stat',round(result_df.ztest_udf[0],2))\
                     .withColumn('p_value',round(result_df.ztest_udf[1],4))\
                     .withColumn('control CI',array(round(result_df.ztest_udf[2],4),round(result_df.ztest_udf[4],4)))\
                     .withColumn('treatment CI',array(round(result_df.ztest_udf[3],4),round(result_df.ztest_udf[5],4)))
  result_df = result_df.drop(result_df.ztest_udf)

  return result_df

# COMMAND ----------

### Model Score Adjustment Across Underwriting Models

#list of different underwriting models
loss_odds_dict = {
    'e_score' : {
		 'f': lambda x,c: 1/(1+np.exp(-c[0]*(x-c[1]))),
		 'c':[3.7921, 1.2382]
	} ,
    'e2_score' : {
		 'f': lambda x,c: 1/(1+np.exp(-c[0]*(x-c[1]))),
		 'c':[5.4512, 1.1943]
	} ,
	'ec_score' : {
		 'f': lambda x,c: c[0]/(1+np.exp(-c[1]*(x-c[2]))) + c[3],
		 'c':[0.1097, 80.02746, 0.0416, 0]
	} ,
	'bt_score' : {
		 'f': lambda x,c: c[0]/(1+np.exp(-c[1]*(x-c[2]))) + c[3],
		 'c':[0.1662, 6.8883, 0.5749, 0]
	} ,
	'lc_score' : {
		 'f': lambda x,c: c[0]/(1+np.exp(-c[1]*(x-c[2]))) + c[3],
		 'c':[2, 10.6703, 1.10740, 0.0034]
	} ,
	'ln_score' : {
		 'f': lambda x,c: c[0]/(1+np.exp(-c[1]*(x-c[2]))) + c[3],
		 'c':[2, 5.0349, 1.3965, 0.0154]
	} ,
	'fico' : {
		 'f': lambda x,c: c[0]*np.sqrt(c[2]*(x-c[3])**2+c[1])/(x-c[3]) - c[0],
		 'c':[-62.7958, -79.1611, 1.0005, 405.1482]
	} ,
 
}

print(loss_odds_dict.keys())

def get_loss_odds(model_name, score, default_value = np.nan):
    """ Computes loss odds from a model score usinging an analytical fit function. 
    Possible models at this time are: e_score, ec_score, bt_score, lc_score,
    ln_score, and fico.

    Returns etimated loss odds.

    :param model_name: name of model
    :type model_name: str
    :param model_score: model score
    :type model_name: float
    :param default_value: value that will be imputed if model_name is missing
    
    :return: The odds of a loan defaulting
    :rtype: float
    """
    if model_name in loss_odds_dict.keys():
        return LOSS_DICT[model_name]['f'](score, loss_odds_dict[model_name]['c'])
    else:
        return score

def get_df_loss_odds(row, default_val=np.nan):
    """ Computes loss odds from analytical fit function for a model score from
    a pandas dataframe. Use: df['loss_odds'] = df.apply(get_df_loss_odds, axis=1) 
    if the columns for 'model_used' and 'model_score' are in the dataframe.
    Possible models at this time are: e_score, ec_score, bt_score, lc_score,
     ln_score, and fico.

    Returns etimated loss odds.

    :param row:  Dictionary or pandas Series with 'model_score' or 'fico' keys.
    :type row: dict{str:float}
    :param default_value: value that will be imputed if model_name is missing
    :type model_name: float
   
    :return: The odds of a loan defaulting
    :rtype: float
    """
    if row['model_used'] in loss_odds_dict.keys():
        return loss_odds_dict(row['model_used'], row['model_score'])
    elif row['fico'].notna():
        return loss_odds_dict('fico', row['fico'])
    else:
        return default_val
      
loss_odds = udf(get_loss_odds, FloatType())

# COMMAND ----------

### Function that returns stratified data after sampling on the original dataframe.
def stratify_data(df_control,df_treatment,param,col_name,random_state=50):
  """
  create strata and proportions of each strata
  """
  level_tr = df_treatment.groupBy(col_name).count()
  level_tr = level_tr.withColumn('PCT',col('count')/sum('count').over(Window.partitionBy()))\
                     .withColumn('NEW_COL',concat(*col_name))  
  strata = level_tr.rdd.map(lambda x: x.NEW_COL).collect()
  proportions = level_tr.rdd.map(lambda x: x.PCT).collect()
  display(level_tr.select('NEW_COL','count','PCT').orderBy('NEW_COL'))

  """Stratifies data according to the values and proportions passed in
  Args: 
      df_control or df_treatment (DataFrame): source data
      col_name (str): The name of the single column in the dataframe that holds the data values that will be used to stratify the data
      strata (list of str): A list of all of the potential values for stratifying e.g. "Male, Graduate", "Male, Undergraduate", "Female, Graduate", "Female, Undergraduate"
      proportions (list of float): A list of numbers representing the desired propotions for stratifying e.g. 0.4, 0.4, 0.2, 0.2, The list values must add up to 1 and must match the number of values in stratify_values
      random_state (int, optional): sets the random_state. Defaults to None.
  Returns:
      DataFrame: a new dataframe based on df_data that has the new proportions represnting the desired strategy for stratifying
  """
  df_control = df_control.withColumn('NEW_COL',concat(*col_name))

  spark = SparkSession.builder.appName('Empty_Dataframe').getOrCreate()
  emp_RDD = spark.sparkContext.emptyRDD()
  columns = df_control.schema
  df_stratified = spark.createDataFrame(data = emp_RDD,schema = columns)

  pos = -1
  #for i in range(2):
  for i in range(len(strata)):
    pos += 1
    if pos == len(strata) - 1:
      ratio_len = int(df_control.count() * param) - df_stratified.count()
    else:
      ratio_len = int(df_control.count() * param * proportions[i])

    df_filtered = df_control[df_control['NEW_COL'] == strata[i]]
    temp = df_filtered.rdd.takeSample(True, ratio_len, random_state) 
    df_temp = spark.createDataFrame(data=temp, schema = df_control.schema)
    df_stratified = df_stratified.unionAll(df_temp)
  
  #display(df_stratified.groupBy('NEW_COL').count().withColumn('PCT',col('count')/sum('count').over(Window.partitionBy())).orderBy('NEW_COL'))
  return df_stratified

# COMMAND ----------

def bucketizer(bins,col_input,col_output,df,user_seg, p):
  bucketizer = Bucketizer(splits = bins,inputCol = col_input, outputCol = col_output)

  df_buck = bucketizer.setHandleInvalid("keep").transform(df).na.drop(subset=[col_output])

  df_buck_agg = df_buck.groupBy('CID','GROUP','USER_FLAG_FIRST',col_output)\
                  .agg(sum('IF_APPROVE').alias('IF_APPROVE'),sum('IF_ACCEPT').alias('IF_ACCEPT'))\
                  .withColumn('IF_APPROVE',when(col('IF_APPROVE') >= 1, lit('1')).otherwise(col('IF_APPROVE')))\
                  .withColumn('IF_ACCEPT',when(col('IF_ACCEPT') >= 1, lit('1')).otherwise(col('IF_ACCEPT')))
  
  df_tr = df_buck_agg.filter((col('GROUP')=='treatment'))
  df_cr = df_buck_agg.filter((col('GROUP')=='control'))
  
  col_t = [col_output]
  param = p
  df_output = stratify_data(df_tr,df_cr,param,col_t)

  tb_strata = df_cr.unionAll(df_output.drop('NEW_COL'))
  #tb_strata = df_output_cr.unionAll(df_output_tr)

  tb_strata_agg = tb_strata.groupBy('GROUP').agg(sum('IF_APPROVE').alias('COUNT'),sum('IF_ACCEPT').alias('SUCCESS'))\
            .withColumn('CR',round(col('SUCCESS')/col('COUNT'),4))

  df_strata = create_result(tb_strata_agg).withColumn('user segment',lit(user_seg))
  return (df_strata,tb_strata)