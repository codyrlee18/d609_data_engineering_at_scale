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

# Script generated for node customer curated
customercurated_node1750453953022 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedibucket-codylee/customer_curated/"], "recurse": True}, transformation_ctx="customercurated_node1750453953022")

# Script generated for node step trainer landing
steptrainerlanding_node1750453893067 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedibucket-codylee/step_trainer_landing/"], "recurse": True}, transformation_ctx="steptrainerlanding_node1750453893067")

# Script generated for node SQL Query
SqlQuery0 = '''
select st.sensorReadingTime, st.serialNumber, st.distanceFromObject
from step_trainer_landing as st
inner join customer_curated as cc
on st.serialNumber = cc.serialNumber
'''
SQLQuery_node1750455024721 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_curated":customercurated_node1750453953022, "step_trainer_landing":steptrainerlanding_node1750453893067}, transformation_ctx = "SQLQuery_node1750455024721")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1750455024721, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750454838653", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1750455207526 = glueContext.getSink(path="s3://stedibucket-codylee/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1750455207526")
AmazonS3_node1750455207526.setCatalogInfo(catalogDatabase="stedidb",catalogTableName="step_trainer_trusted")
AmazonS3_node1750455207526.setFormat("json")
AmazonS3_node1750455207526.writeFrame(SQLQuery_node1750455024721)
job.commit()