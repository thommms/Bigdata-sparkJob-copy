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
public class SparkJobSource4 extends Thread implements Serializable {
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
                .option("url", appConfig.getTableSourceSr4())
                .option("dbtable", appConfig.getJdbcTableNameSr4())
                .option("user", appConfig.getJdbcUserName())
                .option("password", appConfig.getJdbcPassword())
                .load();

        jdbcDF.printSchema();
        Dataset<Row> dataset= jdbcDF.selectExpr("CAST (Id AS String) AS _id",
                "Id","PaymentRefNum","BankId","BankCode",
                "BankCBNCode","BankName","BankName","CurrencyCode",
                "CurrencyName","PaymentDate","ResponseCode","TransactionAmount",
                "ApprovedAmount","Surcharge","SurchargeCurrencyCode","TransactionType",
                "TerminalId","RetrievalReferenceNumber","EncryptedPAN","HashedPAN",
                "MaskedPAN","CustomerName","PaymentChannelId","PaymentChannelName",
                "DepositSlip","TerminalOwnerCode","TerminalOwnerName","CustomerEmail",
                "CustomerMobile","TransactionSetId","TransactionSetName","TerminalOwnerId",
                "CountryCode","CountryName","PaymentMethodId","PaymentMethodName",
                "Destination","MiscData","RequestReference","ProcessingResponseCode",
                "ProcessingResponseDescription","IsInjected","ServiceProviderId","ServiceCode",
                "ServiceName","TransactionStatusId","ServiceProviderCode","AdviceSuccessfullyProcessed",
                "Narration","ReceiptNumber","MerchantSiteDomain","IsInjectProcessing",
                "InjectProcessingCount","RemoteClientName","RemoteClientToken","DeviceTerminalId",
                "RechargePin","PaymentCode","AdditionalResponseData","ValueTokenInfo",
                "ThirdPartyData","STAN","Bin","IsSvaTransaction",
                "AuthorizationId","CustomerBorneFee","AmountLessFee");

        System.out.println("==========================showing "+dataset.count()+" records in the sr4 dataset=============");
        dataset.show(10);

        if (!dataset.rdd().isEmpty()) {

            //Writing to topic
            if ((appConfig.isPushToStream())) {
                System.out.println("======================================Writing to sr4 topic============================");
                try{
                    dataset.alias("value")
                            .toJSON()
                            .write()
                            .format("kafka")
                            .option("kafka.bootstrap.servers", "172.x.x.202:9092")
                            .option("topic", appConfig.getTargetTopicSr4())
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
                mapRDBJavaSession.saveToMapRDB(dataset,appConfig.getTableDestinationsr4(),false);

                System.out.println("=====================DONE PUSHING TO MAPR DB "+dataset.count()+ " for sr4 records================================");

            }

            //save to mapr FS
            if (appConfig.isSaveToFs()) {
                DateTimeFormatter dateformat = DateTimeFormatter.ofPattern("yyyy/MM/dd");
                LocalDateTime now = LocalDateTime.now();

                System.out.println("=============pushing to MapR FS===============");
                String path = appConfig.getFileStoragePathSr4() + "/logs_for -" + dateformat.format(now);

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
