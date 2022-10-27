package com.mongodb.samples.spark;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static avro.shaded.com.google.common.primitives.Ints.asList;
import static java.util.Collections.singletonList;

public class MultipleStageExample {
    private final JavaSparkContext sc;
    private final SparkSession sparkSession;

    public MultipleStageExample () {
        sparkSession = SparkSession.builder()
                .master("local")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection")
                .config("spark.mongodb.output.replaceDocument", "false")
                .config("spark.mongodb.output.ordered", "false")
                .getOrCreate();
        sc = new JavaSparkContext( sparkSession.sparkContext()) ;
    }

    public void process()
    {


        JavaRDD<Document> documents = sc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
                ((Function<Integer, Document>) i -> Document.parse("{test: " + i + "}"));
        WriteConfig writeConfig2 = setWriteDataSource("mongodb://127.0.0.1/test.myCollection", "sample_training", "prework");
        MongoSpark.save(documents, writeConfig2);



        ReadConfig config  = setReadDataSource("mongodb://127.0.0.1/test.myCollection", "sample_training", "grades");
        JavaMongoRDD<Document> customRdd = MongoSpark.load(sc, config);

        // Read pipeline
        JavaMongoRDD<Document> aggregatedRdd = customRdd.withPipeline(
                singletonList(
                        Document.parse("{ $match: { student_id : { $gt : 5 } } }")));


        JavaRDD<GradeVo> doc = aggregatedRdd.map((Function<Document, GradeVo>) doc1 -> {

            GradeVo vo = new GradeVo();
            vo.setStudent_id(doc1.getDouble("student_id"));
            vo.setClass_id( doc1.getDouble("class_id"));
            return vo;
        } );

        JavaRDD<Document> doc2 = doc.map((Function<GradeVo, Document>) vo -> {

            Document d = new Document();
            d.put("student_id", vo.getStudent_id());
            d.put("class_id", vo.getClass_id());
            d.put("last_updated", new Date());
            return d;
        });

        WriteConfig writeConfig = setWriteDataSource("mongodb://127.0.0.1/test.myCollection", "sample_training", "grades2");
        MongoSpark.save(doc2, writeConfig);
    }

    public ReadConfig setReadDataSource(String uri, String database, String collection )  {
        Map<String, String> readOverrides = new HashMap<>();

        readOverrides.put("uri", uri);
        readOverrides.put("database", database);
        readOverrides.put("collection", collection);

        return ReadConfig.create(sc).withOptions(readOverrides);
    }

    public WriteConfig setWriteDataSource ( String uri, String database, String collection) {
        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("uri", uri );
        writeOverrides.put("database", database);
        writeOverrides.put("collection", collection);
        writeOverrides.put("shardKey", "{student_id : 1 }");
        return WriteConfig.create(sc).withOptions(writeOverrides);
    }
}
