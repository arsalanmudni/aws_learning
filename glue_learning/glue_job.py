import sys
from awsglue.transforms import ApplyMapping, Filter, SelectFields, ResolveChoice
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

outputpath = 's3://arsalannewbucket/write/movies_parquet'

datasource0 = glueContext.create_dynamic_frame.from_catalog(database="test-glue-db",
                                                            table_name="read",
                                                            transformation_ctx="datasource0")
applymapping1 = ApplyMapping.apply(frame=datasource0, mappings=[("film", "string", "film", "string"),
                                                                ("genre", "string", "genre", "string"),
                                                                ("lead studio", "string", "lead studio", "string"), (
                                                                "audience score %", "long", "audience score %", "long"),
                                                                ("profitability", "double", "profitability", "double"),
                                                                ("rotten tomatoes %", "long", "rotten tomatoes %",
                                                                 "long"), (
                                                                "worldwide gross", "string", "worldwide gross",
                                                                "string"), ("year", "long", "year", "long")],
                                   transformation_ctx="applymapping1")

converted_df = Filter.apply(frame=applymapping1,
                            f=lambda x: x["Genre"] in ["Romance", "Comedy"])

glueContext.write_dynamic_frame.from_options(frame=converted_df,
                                             connection_type="s3",
                                             connection_options={"path": outputpath},
                                             format="parquet")
job.commit()
