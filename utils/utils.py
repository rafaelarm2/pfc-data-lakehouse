def apply_schema(sdf, schema: object):
    from pyspark.sql.functions import regexp_replace, col, trim
    for column, column_schema in schema.__members__.items():
        if column in sdf.columns:
            if column_schema.type == "float" or column_schema.type == "double":
                sdf = sdf.withColumn(column,regexp_replace(col(column),"\.",""))
                sdf = sdf.withColumn(column,regexp_replace(col(column),",","."))
                sdf = sdf.withColumn(column,regexp_replace(col(column)," ", ""))

            sdf = sdf.withColumnRenamed(column,column_schema.value)
            sdf = sdf.withColumn(column_schema.value,
                                 col(column_schema.value).cast(column_schema.type))

    return sdf

def z_concat(data_frames_list):
    from functools import reduce
    return reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), data_frames_list) 

def union_all(data_frames_list):
    from functools import reduce
    from pyspark.sql.functions import lit
    columns = reduce(lambda x, y : set(x).union(set(y)), [df.columns for df in data_frames_list])

    for i in range(len(data_frames_list)):
        df = data_frames_list[i]
        for column in columns:
            if column not in df.columns:
                df = df.withColumn(column, lit(None))
        data_frames_list[i] = df

    return z_concat(data_frames_list)