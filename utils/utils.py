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