import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.ml.feature import Imputer
spark=SparkSession.builder.appName("pratise").getOrCreate()
## header( specify the row name ) inferschema (data types for the row )
df_Spark=spark.read.csv("files/data.csv",header=True,inferSchema=True)
pn_spark=spark.read.csv("files/test3.csv",header=True,inferSchema=True)

## Column Adding and saving CSV File
df_Spark.withColumn("Age after 2 Years",df_Spark["Age"]+2).toPandas().to_csv("files/test1.csv")

## Remove Columns and saving csv file
df_Spark.drop("Age after 2 Years").toPandas().to_csv("files/AfterRemove.csv")

## Alter Column name
df_Spark.withColumnRenamed("Name","New Name").toPandas().to_csv("files/updatecolumnname.csv")

#df_Spark.show()
## For Row Deletion
df_Spark.drop().toPandas().to_csv("files/afterdelcolumn.csv")

## for Null Value Deletion with thresh (checking null value more than 1 in whole table) subset (checking null value in particular row or column )
df_Spark.na.drop(how="any",thresh=1,subset=["Age"]).toPandas().to_csv("files/update.csv")

##Fill Null values with other values
df_Spark.na.fill("Missing value",subset=["name","Age"]).toPandas().to_csv("files/Replacenullwithsomething.csv")

##Imputer (Remember to take only numeric value) the null value is filled with mean
imputer=Imputer(
    inputCols=["Age"],
    outputCols=["{}_imputed".format(c) for c in ["Age"]]
).setStrategy("mean")
imputer.fit(df_Spark).transform(df_Spark).toPandas().to_csv("files/mean.csv")
##mode (the null value is filled with mode)
imputer=Imputer(
    inputCols=["Age"],
    outputCols=["{}_imputed".format(c) for c in ["Age"]]
).setStrategy("mode")
imputer.fit(df_Spark).transform(df_Spark).toPandas().to_csv("files/mode.csv")

#filter the table

k=df_Spark.filter("Age>22").select(["Name"]).toPandas().to_csv("files/filterpart1.csv")
##Groupby
##maxium salary and their sum
pn_spark.groupBy("name").sum().toPandas().to_csv("files/groupby1.csv")
k=pn_spark.groupBy("departments").sum().toPandas().to_csv("files/groupby2.csv")

##Aggregate
##
pn_spark.agg({"salary":"max"}).toPandas().to_csv("files/aggregate1.csv")






#df_Spark.toPandas().to_csv("datam.csv")