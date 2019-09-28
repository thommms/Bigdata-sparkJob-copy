package com.bigdata_project.demo.jobs;

import com.interswitch.bigdata.demo.configuration.AppConfig;
import com.mapr.db.spark.sql.api.java.MapRDBJavaSession;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

@Component
public class SparkJobSource5 extends Thread implements Serializable {
    Map<String, String> prop;
    @Autowired
    AppConfig appConfig;

    @Autowired
    private MapRDBJavaSession mapRDBJavaSession;

    @Autowired
    Logger logger;

//    @Autowired
//    MaprStreamProducer streamProducer;

    public void run() {
        prop = appConfig.getProps();
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        System.out.println("====================just created a session======================================");
        //loading from jdbc
        Dataset<Row> jdbcDF = sparkSession.read()
                .format("jdbc")
                .option("url", appConfig.getTableSource5())
                .option("dbtable", appConfig.getJdbcTableName5())
                .option("user", appConfig.getJdbcUserName())
                .option("password", appConfig.getJdbcPassword())
                .option("numRows",50)
                .load();
        jdbcDF.printSchema();

        Dataset<Row> dataset = jdbcDF.selectExpr("CAST (transaction_id AS String) AS _id",
                "transaction_id", "tran_seq_id", "account_id",
                "dealer_id", "telco_domain_id", "issuer_domain_id",
                "pan", "terminal_id", "stan",
                "customer_account_no", "terminal_owner_domain_id", "settlement_date",
                "system_settlement_enabled", "card_association_id", "prev_postilion_response_code",
                "prev_postilion_response_message", "e_product_data_info", "additional_info");

        System.out.println("==========================showing the sr5 dataset " +dataset.count()+"==================");
        dataset.show(10);

        if (!dataset.rdd().isEmpty()) {

            //Writing to topic
            if ((appConfig.isPushToStream())) {
                System.out.println("======================================Writing to sr5 topic============================");
                try{
                    dataset.alias("value")
                            .toJSON()
                            .write()
                            .format("kafka")
                            .option("kafka.bootstrap.servers", "172.26.40.202:9092")
                            .option("topic", appConfig.getTargetTopic5())
                            .option("type", "org.apache.flume.source.kafka.KafkaSource")
                            .option("batchsize", 2000)
                            .option("group.id", "spark-jdbc")
                            .option("max.partition.fetch.bytes", 64000)
                            .option("kafka.value.deserializer", "org.apache.kafka.common.serialization.StringSerializer")
                            .mode("append")
                            .save();

                    System.out.println("==============PUSHED "+dataset.count()+ " records to topic successfully=================");
                }
                catch (Exception e) {
                    System.out.println("[!] JOB FAILED");
                    e.printStackTrace();
                }
            }

            //loading to mapr db
            if (appConfig.isSaveToDB()) {
                MapRDBJavaSession mapRDBJavaSession = new MapRDBJavaSession(sparkSession);
                mapRDBJavaSession.saveToMapRDB(dataset, appConfig.getTableDestination5(), false);

                System.out.println("=====================DONE PUSHING TO MAPR DB " + dataset.count() + " for src5 records================================");

            }

            //save to mapr FS
            if (appConfig.isSaveToFs()) {
                DateTimeFormatter dateformat = DateTimeFormatter.ofPattern("yyyy/MM/dd");
                LocalDateTime now = LocalDateTime.now();

                System.out.println("=============pushing to MapR FS===============");
                String path = appConfig.getFileStoragePath5() + "/logs_for -" + dateformat.format(now);

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
