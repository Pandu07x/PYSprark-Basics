from pyspark.sql import *
from pyspark.ml.feature import *
from pyspark.ml.regression import *
spark=SparkSession.builder.appName("ML").getOrCreate()
py_spark=spark.read.csv("files/test1.csv",header=True,inferSchema=True)
assem=VectorAssembler(inputCols=["age","Experience"],outputCol="Independent Features")
output=assem.transform(py_spark)
final=output.select(["Independent Features","Salary"])
train_data,testdata=final.randomSplit([0.75,0.25])
reg=LinearRegression(featuresCol="Independent Features",labelCol="Salary")
regressor=reg.fit(train_data)
data=regressor.evaluate(testdata)
data.predictions.show()
