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

# Script generated for node Amazon S3
AmazonS3_node1750371786189 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedibucket-codylee/customer_trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1750371786189")

# Script generated for node Amazon S3
AmazonS3_node1750371823753 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedibucket-codylee/accelerometer_trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1750371823753")

# Script generated for node Join
Join_node1750371862841 = Join.apply(frame1=AmazonS3_node1750371823753, frame2=AmazonS3_node1750371786189, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1750371862841")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT distinct
    email,
    MAX(phone) AS phone,
    MAX(birthDay) AS birthDay,
    MAX(serialNumber) AS serialNumber,
    MAX(registrationDate) AS registrationDate,
    MAX(lastUpdateDate) AS lastUpdateDate,
    MAX(shareWithResearchAsOfDate) AS shareWithResearchAsOfDate,
    MAX(shareWithPublicAsOfDate) AS shareWithPublicAsOfDate,
    MAX(shareWithFriendsAsOfDate) AS shareWithFriendsAsOfDate
FROM myDataSource
group by email;
'''
SQLQuery_node1750371927027 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1750371862841}, transformation_ctx = "SQLQuery_node1750371927027")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1750371927027, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750369680172", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1750372140356 = glueContext.getSink(path="s3://stedibucket-codylee/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1750372140356")
AmazonS3_node1750372140356.setCatalogInfo(catalogDatabase="stedidb",catalogTableName="customer_curated")
AmazonS3_node1750372140356.setFormat("json")
AmazonS3_node1750372140356.writeFrame(SQLQuery_node1750371927027)
job.commit()