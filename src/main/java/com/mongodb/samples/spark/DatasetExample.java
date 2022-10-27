package com.mongodb.samples.spark;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class DatasetExample {

    private JavaSparkContext sc;
    private SparkSession session ;

    public DatasetExample() {
        session = SparkSession.builder()
                .master("local")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection")
                .getOrCreate();
        sc = new JavaSparkContext( session.sparkContext()) ;
    }



    public void process () {

        Dataset<InspectionVo> explicitDS = MongoSpark.load(sc).toDS(InspectionVo.class);
        explicitDS.printSchema();
        explicitDS.show();

        explicitDS.createOrReplaceTempView("inspections");
        Dataset<Row> ds2 = session.sql("SELECT id, business_name, result FROM inspections WHERE result = 'Pass'");
        ds2.show();


        MongoSpark.write(ds2).option("collection", "inspections2").mode("overwrite").save();
    }

    public ReadConfig setReadDataSource(String uri, String database, String collection ) {
        Map<String, String> readOverrides = new HashMap<String, String>();

        readOverrides.put("uri", uri);
        readOverrides.put("database", database);
        readOverrides.put("collection", collection);

        return ReadConfig.create(sc).withOptions(readOverrides);
    }
}

