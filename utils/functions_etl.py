def apply_schema(sdf, schema: object):
    from pyspark.sql.functions import regexp_replace, col, trim
    for column, column_schema in schema.__members__.items():
        if column in sdf.columns:
            if column_schema.type == "float" or column_schema.type == "double":
                sdf = sdf.withColumn(column, regexp_replace(col(column), "\.", ""))
                sdf = sdf.withColumn(column, regexp_replace(col(column), ",", "."))
                sdf = sdf.withColumn(column, regexp_replace(col(column), " ", ""))

            sdf = sdf.withColumnRenamed(column,column_schema.value)
            sdf = sdf.withColumn(column_schema.value,
                                 col(column_schema.value).cast(column_schema.type))

    return sdf

def process_auxilio(df_auxilio_bronze, schema):
    from pyspark.sql.functions import input_file_name, col, regexp_replace
    df_auxilio_silver = df_auxilio_bronze \
        .withColumnRenamed("GRUPO_CARGO", "NO_GRUPO_CARGO") \
        .withColumnRenamed("CARGO_FUNCAO", "NO_CARGO") \
        .withColumnRenamed("AUX_NATALIDADE", "VALOR_AUXILIO")

    df_auxilio_silver = apply_schema(df_auxilio_silver, schema)

    return df_auxilio_silver