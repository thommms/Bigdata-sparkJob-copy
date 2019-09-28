package com.bigdata_project.demo.jobs;

import com.bigdata_project.demo.configuration.AppConfig;
import com.mapr.db.spark.sql.api.java.MapRDBJavaSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

@Component
public class SparkJobSource1 extends Thread implements Serializable {
    Map<String, String> prop;
    @Autowired
    AppConfig appConfig;

    public void run() {
        prop = appConfig.getProps();
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        //loading from jdbc
        Dataset<Row> jdbcDF = sparkSession.read()
                .format("jdbc")
                .option("url", appConfig.getTableSourceSrc1())
                .option("dbtable", appConfig.getJdbcTableNameSrc1())
                .option("user", appConfig.getJdbcUserName())
                .option("password", appConfig.getJdbcPassword())
                .load();

        jdbcDF.printSchema();
        Dataset<Row> dataset = jdbcDF.selectExpr("CAST (payment_log_id AS String) AS _id",
                "payment_log_id", "channel_id", "channel_name", "coll_acct_num",
                "CAST (bank_coll_fee_percent AS String) AS bank_coll_fee_percent", "reciept_number", "comments",
                "misc_bank_code", "misc_branch_code", "payment_currency", "CAST (tran AS String) AS trans_fee",
             "alternate_payment_log_date");


        System.out.println("==========================showing " + dataset.count() + " records in the src1 dataset=============");
        dataset.show(10);

        if (!dataset.rdd().isEmpty()) {

            //Writing to topic
            if ((appConfig.isPushToStream())) {
                System.out.println("======================================Writing to src1 topic============================");
                try {
                    dataset.alias("value")
                            .toJSON()
                            .write()
                            .format("kafka")
                            .option("kafka.bootstrap.servers", "172.26.40.202:9092")
                            .option("topic", appConfig.getTargetTopicSrc1())
                            .option("type", "org.apache.flume.source.kafka.KafkaSource")
                            .option("batchsize", 2000)
                            .option("group.id", "spark-jdbc")
                            .option("max.partition.fetch.bytes", 64000)
                            .option("kafka.value.deserializer", "org.apache.kafka.common.serialization.StringSerializer")
                            .mode("append")
                            .save();

                    System.out.println("==============PUSHED " + dataset.count() + " records to topic successfully=================");
                } catch (Exception e) {
                    System.out.println("[!] JOB FAILED");
                    e.printStackTrace();
                }
            }

            //loading to mapr db
            if (appConfig.isSaveToDB()) {
                MapRDBJavaSession mapRDBJavaSession = new MapRDBJavaSession(sparkSession);
                mapRDBJavaSession.saveToMapRDB(dataset, appConfig.getTableDestinationSrc1(), false);

                System.out.println("=====================DONE PUSHING TO MAPR DB " + dataset.count() + " for src records================================");

            }

            //save to mapr FS
            if (appConfig.isSaveToFs()) {
                DateTimeFormatter dateformat = DateTimeFormatter.ofPattern("yyyy/MM/dd");
                LocalDateTime now = LocalDateTime.now();

                System.out.println("=============pushing to MapR FS===============");
                String path = appConfig.getFileStoragePathSrc1() + "/logs_for -" + dateformat.format(now);

                //write to FS
                dataset.write().parquet(path);

                //Alternatively to write to FS
//                dataset.write()
//                        .format("parquet")
//                        .mode(org.apache.spark.sql.SaveMode.Append)
//                        .save(path);

                System.out.println("=================DONE PUSHING TO FS for today " + dateformat.format(now) + "================");
            }
        }


    }
}
