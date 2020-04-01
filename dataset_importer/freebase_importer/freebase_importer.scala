import spark.implicits._



def preprocess_freebase(path: String) : Dataset[Triplet] = {
    val freebase = sc.textFile(path)
    val triplets = freebase.map(line => {
    val parts = line.split("\t")
    Triplet(parts(0), parts(1), parts(2))
    }).cache

    val ent = triplets
                .flatMap(t => if (t.subj==t.obj) Seq((t.subj, 1))  else Seq((t.subj, 1), (t.obj, 1)))
                .reduceByKey(_+_).filter{case(ent, count) => count >= 5}
                .map{case (ent, count) => ent}
                .toDF("ent")
                .cache()

    val rel = sc.broadcast(triplets
            .flatMap(t => Seq((t.rel, 1)))
            .reduceByKey(_+_)
            .filter{case(rel, count) => count >= 5}
            .map{case (rel, count) => rel}
            .toDF("rel_filtered").as[String].collect())

    val filteredTriplets = triplets
        .toDF("subj", "rel", "obj")
        .join(ent, 'subj === 'ent)
        .drop("ent")
        .join(ent, 'obj === 'ent)
        .drop("ent")
        .filter('rel.isin(rel.value.toSeq: _*))
        .as[Triplet]

    filteredTriplets
}

val filteredTriplets = preprocess_freebase("/user/m.vinyes/freebase")


// replace the hdfs path by your paths
create_dataset(filteredTriplets, 4, "/user/r.beaumont/freebase_refactored")






