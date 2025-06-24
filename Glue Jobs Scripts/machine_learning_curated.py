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

# Script generated for node accelerometer trusted
accelerometertrusted_node1750456468220 = glueContext.create_dynamic_frame.from_catalog(database="stedidb", table_name="accelerometer_trusted", transformation_ctx="accelerometertrusted_node1750456468220")

# Script generated for node step trainer trusted
steptrainertrusted_node1750456429711 = glueContext.create_dynamic_frame.from_catalog(database="stedidb", table_name="step_trainer_trusted", transformation_ctx="steptrainertrusted_node1750456429711")

# Script generated for node SQL Query
SqlQuery0 = '''
select *
from step_trainer_trusted as st
inner join accelerometer_trusted as at
on at.timestamp = st.sensorReadingTime
'''
SQLQuery_node1750456523205 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_trusted":steptrainertrusted_node1750456429711, "accelerometer_trusted":accelerometertrusted_node1750456468220}, transformation_ctx = "SQLQuery_node1750456523205")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1750456523205, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750725505864", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1750725666856 = glueContext.getSink(path="s3://stedibucket-codylee/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1750725666856")
AmazonS3_node1750725666856.setCatalogInfo(catalogDatabase="stedidb",catalogTableName="machine_learning_curated")
AmazonS3_node1750725666856.setFormat("json")
AmazonS3_node1750725666856.writeFrame(SQLQuery_node1750456523205)
job.commit()