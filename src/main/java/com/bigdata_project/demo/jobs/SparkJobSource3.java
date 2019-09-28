package com.bigdata_project.demo.jobs;

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
public class SparkJobSource3 extends Thread implements Serializable {
    Map<String, String> prop;
    @Autowired
    AppConfig appConfig;

//    @Autowired
//    Utilities utilities;
//
//    @Autowired
//    MapRDBRepository keyValueRepository;

    public void run() {
        prop = appConfig.getProps();
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        //loading from jdbc
        Dataset<Row> jdbcDF = sparkSession.read()
                .format("jdbc")
                .option("url", appConfig.getTableSourceS3())
                .option("dbtable", appConfig.getJdbcTableNameSr4())
                .option("user", appConfig.getJdbcUserName())
                .option("password", appConfig.getJdbcPassword())
                .load();

        jdbcDF.printSchema();
        Dataset<Row> dataset = jdbcDF.selectExpr("CAST (post_tran_id AS String) AS _id",
                "post_tran_id", "post_tran_cust_id", "settle_entity_id", "batch_nr",
                "prev_post_tran_id", "next_post_tran_id", "sink_node_name", "CAST (tran_postilion_originated AS String)",
                "CAST (tran_completed AS String)", "message_type", "tran_type", "tran_nr",
            "pt_pos_terminal_operator", "source_node_key",
                "proc_online_system_id", "from_account_id_cs", "to_account_id_cs");

        System.out.println("==========================showing " + dataset.count() + " records in the dataset=============");
        dataset.show(10);

        if (!dataset.rdd().isEmpty()) {

            //Writing to topic
            if ((appConfig.isPushToStream())) {
                System.out.println("======================================Writing to topic============================");
                try {
                    dataset.alias("value")
                            .toJSON()
                            .write()
                            .format("kafka")
                            .option("kafka.bootstrap.servers", "172.x.x.202:9092")
                            .option("topic", appConfig.getTargetTopic3())
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
                mapRDBJavaSession.saveToMapRDB(dataset, appConfig.getTableDestination3(), false);
                System.out.println("=====================DONE PUSHING TO MAPR DB " + dataset.count() + " for src3 records================================");

            }

            //save to mapr FS
            if (appConfig.isSaveToFs()) {
                DateTimeFormatter dateformat = DateTimeFormatter.ofPattern("yyyy/MM/dd");
                LocalDateTime now = LocalDateTime.now();

                System.out.println("=============pushing to MapR FS===============");
            String path = appConfig.getFileStoragePath3()+ "/logs_for_5 -"+dateformat.format(now);
//            Configuration conf = new Configuration();
//            conf.set("fs.defaultFS",path);
//            try {
//                FileSystem.get(conf);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }

                //write to FS
                dataset.write().parquet(path);

                //Alternatively to write to FS
//                dataset.write()
//                        .format("parquet")
//                        .mode(org.apache.spark.sql.SaveMode.Append)
//                        .save(path);

                System.out.println("=================DONE PUSHING TO FS for today " + dateformat.format(now) + "================");
            }

//        //pushing to another db
//        System.out.println("=============i want to push to another db from tran table===============");
//        Dataset readingTran=sparkSession.readStream()
//                .format("kafka")
//                .option("subscribe", appConfig.getTargetTopicSwtchTran())
//                .option("kafka.bootstrap.servers", "172.26.40.202:9092")
//                .option("startingOffsets", "earliest")
//                .option("batchDurationMillis", 5000)
//                .option("batchSize", 2000)
//                .option("checkpointLocation",appConfig.getCheckpointLocation())
//                .option("type", "org.apache.flume.source.kafka.KafkaSource")
//                .option("failOnDataLoss", false)
//                .load()
//                .selectExpr("CAST(value as STRING)");
//        System.out.println("======================ABOUT WRITING TO THE DB======================");
//          StreamingQuery query= readingTran.writeStream().
//                    format(MapRDBSourceConfig.Format())
//                    .option(MapRDBSourceConfig.TablePathOption(), "/db/ss3/post_trans_new")
//                    .option(MapRDBSourceConfig.CreateTableOption(), false)
//                    .option("checkpointLocation",appConfig.getTableDestinationss3())
//                    .option(MapRDBSourceConfig.IdFieldPathOption(), "_id")
//                    .outputMode("append")
//                    .start();
//        try {
//            query.awaitTermination();
//        } catch (StreamingQueryException e) {
//            e.printStackTrace();
//        }
//        System.out.println("==========PUSHED TO THE NEW LOCATION SUCCESSFULLY");
            //
//            JavaRDD<Row> r = dataset.toJavaRDD();
//            Row lastRow = r.max(comparator);
//            Long lastId = lastRow.getLong(lastRow.fieldIndex(appConfig.getIdentityFieldNameSr3()));
//
//            KeyValueItem s = new KeyValueItem();
////            s.setKey(utilities.buildKeyScheme());
//            s.setValue(String.valueOf(lastId));
//            keyValueRepository.update(s);
//        } /*else {
//            long nextId = 0;
//            try {
//                nextId = dataProvider.getLasId(checkpoint, 0L);
//                if (nextId > 0L) {
//                    KeyValueItem s = new KeyValueItem();
//                    s.setKey(utilities.buildKeyScheme());
//                    s.setValue(String.valueOf(nextId));
//                    keyValueRepository.update(s);
//                }
//            } catch (Exception e) {
//                System.out.println("Record Polling Returned 0 Results");
//                System.out.println(e.getMessage());
//                logger.info(e.getMessage());
//            }
//        }*/


        }
    }
}