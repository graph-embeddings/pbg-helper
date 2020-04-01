import spark.implicits._

val freebase = spark.read.parquet("/user/r.beaumont/the_full_freebase")

val ent = spark.read.parquet("/user/m.vinyes/full_freebase_entities_parquet").toDF("index", "ent", "partition")
val ent_names =freebase.filter('rel === "<http://www.w3.org/2000/01/rdf-schema#label>" && 'obj.contains("@en"))
.dropDuplicates(Seq("subj"))
.withColumn("name", substring_index(substring_index('obj, "\"", -2), "\"", 1)).drop("obj").drop("rel")
val frequency = freebase.groupBy("subj").count()

val covered_names = ent_names.join(ent, 'subj === 'ent).drop("subj").join(frequency, 'subj === 'ent).drop("subj")

covered_names.repartition(1).write.mode("overwrite").parquet("/user/r.beaumont/freebase_names")

val rel = spark.read.parquet("/user/m.vinyes/full_freebase_relations_parquet")
val frequency_relation = freebase.groupBy("rel").count()
val relation_names = ent_names.join(rel, 'relation === 'subj).drop("subj")
.join(frequency_relation, 'rel === 'relation).drop("rel").withColumnRenamed("relation", "ent").withColumn("partition",lit(0))

relation_names.repartition(1).write.mode("overwrite").parquet("/user/r.beaumont/freebase_relation_names")