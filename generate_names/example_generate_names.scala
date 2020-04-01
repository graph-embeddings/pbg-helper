import org.apache.spark.sql.DataFrame

def generateEntityNames(train: DataFrame, entitiesPath: String, entityNamesOutputPath: String) = {
    val ent = spark.read.parquet(entitiesPath) // DataFrame with columns: "index", "ent", "partition"
    val ent_names = ent.withColumn("name", 'ent)
    val ent_left = train.select("subj", "part_left").withColumnRenamed("subj", "ent_index").withColumnRenamed("part_left", "part")
    val ent_right = train.select("obj", "part_right").withColumnRenamed("obj", "ent_index").withColumnRenamed("part_right", "part")
    val ent_union = ent_left.union(ent_right)
    val frequency = ent_union.groupBy("ent_index", "part").count()
    val entity_names = ent_names.join(frequency, 'index === 'ent_index && 'partition === 'part).drop("part", "ent_index")

    // entity_names parquet file contains a DataFrame with columns "name", "ent", "count", "index", "partition"
    // for sanity check you can run in your sparkshell: entity_names.sort(-'count).show()
    entity_names.repartition(1).write.mode("overwrite").parquet(entityNamesOutputPath)
}

def generateRelationNames(train: DataFrame, relationPath: String, relationNamesOutputPath: String) = {
    val rel = spark.read.parquet(relationPath) // DataFrame with columns: "index", "relation"
    val rel_names = rel.withColumn("name", 'relation)
    val frequency = train.select("rel").groupBy("rel").count()
    val relation_names = rel_names.join(frequency, 'index === 'rel)
                                  .drop("rel")
                                  .withColumnRenamed("relation", "ent")
                                  .withColumn("partition", lit(0))


    // entity_names parquet file contains a DataFrame with columns "name", "ent", "count", "index", "partition"
    // here "ent" column refers to relation
    // for sanity check you can run in your sparkshell: relation_names.sort(-'count).show()
    relation_names.repartition(1).write.mode("overwrite").parquet(relationNamesOutputPath)
}

def generateNames(datasetPath: String) = {
    val relationPath = datasetPath + "/relations"
    val entitiesPath = datasetPath + "/entities"
    val tripletsTrainPath = datasetPath + "/train"
    val relationNamesOutputPath = datasetPath + "/relations_names"
    val entityNamesOutputPath = datasetPath + "/entity_names"

    val train =  spark.read.parquet(tripletsTrainPath) // DataFrame with columns: "subj", "rel", "obj", "part_left", "part_right"

    generateEntityNames(train, entitiesPath, entityNamesOutputPath)
    generateRelationNames(train, relationPath, relationNamesOutputPath)
}


generateNames("/user/r.beaumont/freebase")