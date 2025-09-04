from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import col,dense_rank,when,rank

spark=SparkSession.builder.appName("PagilaSpark")\
    .master("spark://master:7077")\
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar")\
    .getOrCreate()
jdbc_url = "jdbc:postgresql://pagila:5432/postgres"
pg_properties = {"user": "postgres", "password": "123456", "driver": "org.postgresql.Driver"}


actor=spark.read.jdbc(url=jdbc_url,table="public.actor",properties=pg_properties)
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


category_counts=film_category.select("category_id").groupBy("category_id").count()
task_1_result=category_counts.join(on="category_id", how="inner",other=category)\
                             .orderBy(col("count").desc()).drop("last_update")\
                             .withColumnRenamed("count","number_of_films")\
                             .withColumnRenamed("name","category")

task_1_result.show()

film_actor_data=film_actor.select("film_id","actor_id")\
                          .join(actor.select("actor_id","first_name","last_name"),on="actor_id",how="inner")\
                          .join(film.select("film_id"),on="film_id",how="inner")
film_rental_rate=rental.select("inventory_id")\
                       .join(other=inventory.select("inventory_id","film_id"),on="inventory_id",how="inner")\
                       .groupBy("film_id").count()
task_2_result=film_actor_data.select("film_id","actor_id" ,"first_name", "last_name")\
                             .join(other=film_rental_rate,on="film_id",how="inner")\
                             .groupBy("actor_id" ,"first_name", "last_name")\
                             .sum("count").sort(col("sum(count)").desc())\
                             .withColumnRenamed("sum(count)","sum").limit(10)
task_2_result.show()


task_3_result=payment.select("amount","rental_id")\
                     .join(other=rental,on="rental_id",how="inner")\
                     .join(other=inventory,on="inventory_id",how="inner")\
                     .join(other=film_category,on="film_id",how="inner")\
                     .join(other=category,on="category_id",how="inner")\
                     .groupBy("category_id","name").sum("amount").orderBy(col("sum(amount)").desc())\
                     .select("name").limit(1)
task_3_result.show()

task_4_result=film.join(other=inventory,on="film_id",how="left_anti").select("title")
task_4_result.show()

all_movies = film_category.join(other=category.filter("name='Children'")\
                                ,on="category_id",how="inner").select("film_id")
film_count = film_actor.join(other=all_movies,on="film_id",how="inner")\
                       .groupBy("actor_id").count()

window1=Window.orderBy(col("count").desc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
task_5_result = actor.join(other=film_count,on="actor_id",how="left")\
                     .select("first_name", "last_name", "count").fillna(-1)\
                     .withColumn("rank",dense_rank().over(window1))\
                     .filter(col("rank")<=3)
task_5_result.show(task_5_result.count())

task_6_result=customer.select("active","address_id").join(other=address,on="address_id",how="inner")\
                      .withColumn("inactive", 1 - col("active"))
task_6_result=task_6_result.join(other=city,on="city_id",how="inner").orderBy(col("inactive").desc())\
                           .select("city","active","inactive").distinct()
task_6_result.show()

film_time = rental.join(other=inventory,on="inventory_id",how="inner")\
                .join(other=film_category,on="film_id",how="inner")\
                .join(other=category,on="category_id",how="inner")\
                .withColumn("time",col("return_date").cast("long")-col("rental_date").cast("long"))\
                .select("customer_id","name","time").withColumnRenamed("name","category")


city_category_time = film_time.join(other=customer,on="customer_id",how="inner")\
                            .join(other=address,on="address_id",how="inner")\
                            .join(other=city,on="city_id",how="inner")\
                            .groupBy("city","category").sum("time")\
                            .select("city","category","sum(time)")

city_category_max_time = city_category_time.withColumn("city_type",when(col("city").startswith("A"),"A")\
                                                       .when(col("city").contains("-"),"-")\
                                                       .otherwise(None))
window2=Window.orderBy(col("total_time").desc()).partitionBy("city_type")
task_7_result=city_category_max_time.groupby("city_type","category").sum("sum(time)")\
                             .withColumnRenamed("sum(sum(time))","total_time")\
                             .withColumn("rank",rank().over(window2))\
                             .filter((col("rank")==1) & ((col("city_type")=="A") | (col("city_type")=="-")))\
                             .select("city_type","category",((col("total_time")/60/60/24).cast("int")).alias("total_time"))
task_7_result.show()


task_1_result.cache()
print("\n\n\n\nCount:", task_1_result.count())
task_1_result.write.mode("overwrite").option("header", "true").csv("task_output/task_1_result.csv")

task_2_result.cache()
print("\n\n\n\nCount:", task_2_result.count())
task_2_result.write.mode("overwrite").option("header", "true").csv("task_output/task_2_result.csv")

task_3_result.cache()
print("\n\n\n\nCount:", task_3_result.count())
task_3_result.write.mode("overwrite").option("header", "true").csv("task_output/task_3_result.csv")

task_4_result.cache()
print("\n\n\n\nCount:", task_4_result.count())
task_4_result.write.mode("overwrite").option("header", "true").csv("task_output/task_4_result.csv")

task_5_result.cache()
print("\n\n\n\nCount:", task_5_result.count())
task_5_result.write.mode("overwrite").option("header", "true").csv("task_output/task_5_result.csv")

task_6_result.cache()
print("\n\n\n\nCount:", task_6_result.count())
task_6_result.write.mode("overwrite").option("header", "true").csv("task_output/task_6_result.csv")

task_7_result.cache()
print("\n\n\n\nCount:", task_7_result.count())
task_7_result.write.mode("overwrite").option("header", "true").csv("task_output/task_7_result.csv")