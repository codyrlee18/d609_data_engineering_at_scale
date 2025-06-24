import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node customer
customer_node1750261363800 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedibucket-codylee/customer_trusted/"], "recurse": True}, transformation_ctx="customer_node1750261363800")

# Script generated for node accelerometer
accelerometer_node1750261321888 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedibucket-codylee/accelerometer_landing/"], "recurse": True}, transformation_ctx="accelerometer_node1750261321888")

# Script generated for node Join
Join_node1750261355037 = Join.apply(frame1=accelerometer_node1750261321888, frame2=customer_node1750261363800, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1750261355037")

# Script generated for node SQL Query
SqlQuery0 = '''
select user, timestamp, x, y, z
from myDataSource
where timestamp >= shareWithResearchAsOfDate;

 
'''
SQLQuery_node1750262173171 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1750261355037}, transformation_ctx = "SQLQuery_node1750262173171")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1750262173171, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750261308121", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1750261603143 = glueContext.getSink(path="s3://stedibucket-codylee/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1750261603143")
AmazonS3_node1750261603143.setCatalogInfo(catalogDatabase="stedidb",catalogTableName="accelerometer_trusted")
AmazonS3_node1750261603143.setFormat("json")
AmazonS3_node1750261603143.writeFrame(SQLQuery_node1750262173171)
job.commit()