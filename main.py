from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("PagilaSpark")\
    .config("spark.jars", "jars/postgresql-42.7.3.jar")\
    .getOrCreate()
jdbc_url = "jdbc:postgresql://localhost:5432/pagila"
pg_properties = {"user": "postgres", "password": "123456", "driver": "org.postgresql.Driver"}


actor=spark.read.jdbc(url=jdbc_url,table="actor",properties=pg_properties)
address=spark.read.jdbc(url=jdbc_url,table="address",properties=pg_properties)
category=spark.read.jdbc(url=jdbc_url,table="category",properties=pg_properties)
city=spark.read.jdbc(url=jdbc_url,table="city",properties=pg_properties)
country=spark.read.jdbc(url=jdbc_url,table="country",properties=pg_properties)
customer=spark.read.jdbc(url=jdbc_url,table="customer",properties=pg_properties)
film=spark.read.jdbc(url=jdbc_url,table="film",properties=pg_properties)
film_actor=spark.read.jdbc(url=jdbc_url,table="film_actor",properties=pg_properties)
film_category=spark.read.jdbc(url=jdbc_url,table="film_category",properties=pg_properties)
inventory=spark.read.jdbc(url=jdbc_url,table="inventory",properties=pg_properties)
language=spark.read.jdbc(url=jdbc_url,table="language",properties=pg_properties)
payment=spark.read.jdbc(url=jdbc_url,table="payment",properties=pg_properties)
rental=spark.read.jdbc(url=jdbc_url,table="rental",properties=pg_properties)
