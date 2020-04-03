#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  3 13:07:34 2020

@author: rahul
"""

from pyspark.sql import SparkSession

spark= SparkSession.builder.appName('Basics').getOrCreate()

#Read values from csv file
#inferschema: get datatypes of all the columns etc
#header: take the first row of the csv file as the column
df= spark.read.csv('census.csv', inferSchema= True, header=True)

#use show method to visualize the dataframe.
print(df.show())

#Shows the schema of the dataframe
print(df.printSchema())

#show the summary of the columns with numerical values
print(df.describe().show())


df.select('_c0')  #Returns as a dataframe
df['_c0']       #Returns as a column object
df.select(['_c0','_c1'])

#get distinct values from a column
df.select('age').distinct().show()
#We shoudl use select most of the time since it returns a dataframe

#Creates new column new_c0 with the data of df['_c0']
df.withColumn('new_c0', df['_c0'])
#Renaming a column
df.withColumnRenamed('_c1', 'new_c1')


#Registered the dataframe as a SQL View 'people'so that we can fetch values by SQL Queries
df.createOrReplaceTempView('people')

#Writing SQL Queries and it returns a dataframe
results= spark.sql('SELECT * FROM people')
type(results)





#Fitering the Dataframe

#filter based on conditions
df.filter('age >10').show()
df.filter('age >10').select('workclass').show()


#Another way
df.filter(df['age'] >10).show()

#adding multiple conditions in the dataframe
df.filter( (df['age']>10) & (df['age']<20)).show()

#collect the data into another variable using collect method; it returns a list
result = df.filter( (df['age']>10) & (df['age']<20)).collect()
type(result)



#GroupBy conditions
df.groupBy('age').sum().show()
df.select('age').show()


#Additional functions
from pyspark.sql.functions import format_number, stddev
#find std of column age and give the column another name
result=  df.select(stddev('age').alias('new_age'))
#convert decimal to 2 digits
result.select(format_number('new_age',2)).show()

#ascending order
df.orderBy('age')
#descending order
df.orderBy(df['age'].desc())
