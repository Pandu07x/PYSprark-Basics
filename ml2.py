from pyspark.sql import *
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

spark=SparkSession.builder.appName("ML2").getOrCreate()
df = spark.read.csv("file/tip.csv",header=True,inferSchema=True)

indes=StringIndexer(inputCols=["sex","time","day","smoker"],outputCols=["edited_sex","edited_dinner","edited_day","edited_smoker"])
k=indes.fit(df).transform(df)
l=k.select(["edited_sex","edited_dinner","edited_day","edited_smoker","tip"])
assem=VectorAssembler(inputCols=["edited_sex","edited_dinner","edited_day","edited_smoker","tip"],outputCol="Independent Index")
dk=assem.transform(k)
s=dk.select(["Independent Index","total_bill"])

reg,test_data=s.randomSplit([0.75,0.25])
regressor=LinearRegression(featuresCol="Independent Index",labelCol="total_bill")
rege=regressor.fit(reg)
predect=rege.evaluate(test_data)
predect.predictions.show()
