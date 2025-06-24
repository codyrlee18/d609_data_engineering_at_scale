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
AmazonS3_node1750256888735 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedibucket-codylee/customer_landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1750256888735")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null
'''
SQLQuery_node1750257320882 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AmazonS3_node1750256888735}, transformation_ctx = "SQLQuery_node1750257320882")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1750257320882, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750256739987", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1750257611117 = glueContext.getSink(path="s3://stedibucket-codylee/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1750257611117")
AmazonS3_node1750257611117.setCatalogInfo(catalogDatabase="stedidb",catalogTableName="customer_trusted")
AmazonS3_node1750257611117.setFormat("json")
AmazonS3_node1750257611117.writeFrame(SQLQuery_node1750257320882)
job.commit()