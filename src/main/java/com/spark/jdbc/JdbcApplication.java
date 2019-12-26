package com.spark.jdbc;

import java.nio.file.Paths;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class JdbcApplication {

	public static void main(String[] args) {
		SpringApplication.run(JdbcApplication.class, args);

        System.setProperty("hadoop.home.dir", Paths.get("").toAbsolutePath() + "/src/main/resources/hadoop");

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SparkApp")
                .setSparkHome("D:/develop/spark-jdbc/src/main/resources/");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd = sc.textFile("input");

        // rdd.persist(StorageLevel.DISK_ONLY());

        rdd.foreach(x -> System.out.println(x));

        // rdd.saveAsTextFile("output");

        // Output Data Schema: struct<value: string>
        // SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic
        // example").getOrCreate();
        // Dataset<Row> df = spark.read().csv("report.csv");
        // df.show();
        // Displays the content of the DataFrame to stdout
        // +----+-------+ // | age| name| // +----+-------+ // |null|Michael| // | 30|
        // Andy| // | 19| Justin| // +----+-------+

        System.exit(0);

	}

}
