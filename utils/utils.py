def apply_schema(sdf, schema: object):
    from pyspark.sql import functions as F
    for column, column_schema in schema.__members__.items():
        if column in sdf.columns:
            if column_schema.type == "float" or column_schema.type == "double":
                sdf = sdf.withColumn(column,F.regexp_replace(F.col(column),".",""))
                sdf = sdf.withColumn(column,F.regexp_replace(F.col(column),",","."))

            sdf = sdf.withColumnRenamed(column,column_schema.value)
            sdf = sdf.withColumn(column_schema.value,
                                F.col(column_schema.value).cast(column_schema.type))

    return sdf
