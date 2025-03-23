from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("books")\
        .getOrCreate()

    print("read books.csv ... ")
    path_books = "dataset.csv"
    df_books = spark.read.csv(path_books, header=True, inferSchema=True)
    
    # Renombramos las columnas según sea necesario
    df_books = df_books.withColumnRenamed("publication_date", "pub_date")
    
    # Creamos una vista temporal para consultas SQL
    df_books.createOrReplaceTempView("books")
    
    # Realizamos una consulta SQL básica para describir la tabla
    query = 'DESCRIBE books'
    spark.sql(query).show(20)
    
    # Consulta 1: Obtener libros con una calificación promedio superior a 4.0 ordenados por el título
    query = """SELECT title, authors, average_rating 
               FROM books 
               WHERE average_rating > 4.0 
               ORDER BY average_rating DESC"""
    df_books_high_rating = spark.sql(query)
    df_books_high_rating.show(20)
    
    # Consulta 2: Obtener libros publicados entre 2000 y 2010 ordenados por la fecha de publicación
    query = 'SELECT title, pub_date FROM books WHERE pub_date BETWEEN "2000-01-01" AND "2010-12-31" ORDER BY pub_date'
    df_books_2000_2010 = spark.sql(query)
    df_books_2000_2010.show(20)
    
    # Convertimos los resultados a JSON y los guardamos en un archivo
    results = df_books_2000_2010.toJSON().collect()
    with open('results/books_2000_2010.json', 'w') as file:
        json.dump(results, file)

    # Consulta 3: Contar cuántos libros existen por idioma (language_code)
    query = 'SELECT language_code, COUNT(language_code) FROM books GROUP BY language_code'
    df_books_by_language = spark.sql(query)
    df_books_by_language.show()

    spark.stop()
