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
public class SparkJobSource2 extends Thread implements Serializable {
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
                .option("url", appConfig.getTableSourceSrc2())
                .option("dbtable", appConfig.getJdbcTableNameSrc2())
                .option("user", appConfig.getJdbcUserName())
                .option("password", appConfig.getJdbcPassword())
                .load();

        jdbcDF.printSchema();
        Dataset<Row> dataset= jdbcDF.selectExpr("CAST (post_tran_cust_id AS String) AS _id",
                "post_tran_cust_id","source_node_name","draft_capture","pan",
                "card_seq_nr","expiry_date","service_restriction_code","terminal_id",
                "terminal_owner","card_acceptor_id_code","mapped_card_acceptor_id_code","merchant_type",
                "card_acceptor_name_loc","address_verification_data","address_verification_result","check_data",
                "totals_group","card_product","pos_card_data_input_ability","pos_cardholder_auth_ability",
                "pos_card_capture_ability","pos_operating_environment","pos_cardholder_present","pos_card_present",
                "pos_card_data_input_mode","pos_cardholder_auth_method","pos_cardholder_auth_entity","pos_card_data_output_ability",
                "pos_terminal_output_ability","pos_pin_capture_ability","pos_terminal_operator","pos_terminal_type",
                "pan_search","pan_encrypted","pan_reference","card_acceptor_id_code_cs");

        System.out.println("==========================showing "+dataset.count()+" records in the dataset=============");
        dataset.show(10);

        if (!dataset.rdd().isEmpty()) {

            //Writing to topic
            if ((appConfig.isPushToStream())) {
                System.out.println("======================================Writing to topic============================");
                try{
                    dataset.alias("value")
                            .toJSON()
                            .write()
                            .format("kafka")
                            .option("kafka.bootstrap.servers", "172.26.40.202:9092")
                            .option("topic", appConfig.getTargetTopicSwtch())
//                        .option("type", "org.apache.flume.source.kafka.KafkaSource")
//                        .option("batchSize", 2000)
//                        .option("group.id", "spark-jdbc")
                            .option("max.partition.fetch.bytes", 64000)
//                        .option("kafka.value.deserializer", "org.apache.kafka.common.serialization.StringSerializer")
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
                mapRDBJavaSession.saveToMapRDB(dataset, appConfig.getTableDestinationSr2(), false);

                System.out.println("=====================DONE PUSHING TO MAPR DB " + dataset.count() + " for sr2 records================================");
            }

            //save to mapr FS
            if (appConfig.isSaveToFs()) {
                DateTimeFormatter dateformat = DateTimeFormatter.ofPattern("yyyy/MM/dd");
                LocalDateTime now = LocalDateTime.now();

                System.out.println("=============pushing to MapR FS===============");
                String path = appConfig.getFileStoragePathSr2() + "/logs_for -" + dateformat.format(now);

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