from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
dynamicFrame = glueContext.create_dynamic_frame.from_options(
    connection_type = "s3", 
    connection_options = {"paths": ["s3://firstproject112/userdata1.parquet"]}, 
    format = "parquet"
)
dynamicFrame.show()
dataframe=dynamicFrame.toDF()
dataframe = dataframe.withColumn('Double_salary', dataframe.salary *2)
dataframe.head()
dataframe.write.format("csv").mode("append").save("s3://output1112/output1")
job.commit()