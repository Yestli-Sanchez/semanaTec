from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("books")\
        .getOrCreate()

    print("read dataset.csv ... ")
    path_people="dataset.csv"
    df_people = spark.read.csv(path_people,header=True,inferSchema=True)
    df_people = df_people.withColumnRenamed("num_pages", "paginas")
    df_people.createOrReplaceTempView("books")
    query='DESCRIBE books'
    spark.sql(query).show(20)

    query='SELECT name, authors,`paginas` FROM books WHERE `paginas` > 10 ORDER BY `paginas`'
    df_books_paginas = spark.sql(query)
    df_books_paginas.show(20)
    results = df_books_paginas.toJSON().collect()
    #print(results)
    df_books_paginas.write.mode("overwrite").json("results")
    #df_people_1903_1906.coalesce(1).write.json('results/data_merged.json')
    with open('results/data.json', 'w') as file:
        json.dump(results, file)
    spark.stop()
