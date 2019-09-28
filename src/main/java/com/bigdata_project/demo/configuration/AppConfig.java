package com.bigdata_project.demo.configuration;

import com.bigdata_project.demo.SparkJobApplication;
import com.mapr.db.spark.sql.api.java.MapRDBJavaSession;
import lombok.Data;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.ojai.store.Connection;
import org.ojai.store.DriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

@Data
@Component
@PropertySource("classpath:application.properties")
@ConfigurationProperties("")
public class AppConfig implements Serializable {
    // private Environment environment;
    @Value("${jdbc.userName}")
    private String jdbcUserName;

    @Value("${jdbc.password}")
    private String jdbcPassword;

    @Value("${mapr.saveToFs}")
    private boolean saveToFileStorage;

    @Value("${mapr.saveToDB}")
    private boolean saveToDB;

    @Value("${mapr.pushToStream}")
    private boolean pushToStream;

    @Value("${mapr.saveToFs}")
    private boolean saveToFs;

    @Value("${mapr.progress_tracking}")
    private String checkpointLocation;

    @Value("${mapr.ss1.fileStoragePath}")
    private String fileStoragePathSrc1;
    @Value("${mapr.ss2.fileStoragePath}")
    private String fileStoragePathSrc2;
    @Value("${mapr.ss3.fileStoragePath}")
    private String fileStoragePathSrc3;
    @Value("${mapr.ss4.fileStoragePath}")
    private String fileStoragePathSrc4;
    @Value("${mapr.ss5.fileStoragePath}")
    private String fileStoragePathSrc5;

    @Value("${jdbc.ss1.tableName}")
    private String jdbcTableNameSrc1;
    @Value("${jdbc.ss2.tableName}")
    private String jdbcTableNameSrc2;
    @Value("${jdbc.ss3.tableName}")
    private String jdbcTableNameSrc3;
    @Value("${jdbc.ss4.tableName}")
    private String jdbcTableNameSrc4;
    @Value("${jdbc.ss5.tableName}")
    private String jdbcTableNameSrc5;

    @Value("${mapr.ss1.streamingtopic}")
    private String targetTopicSrc1;
    @Value("${mapr.ss2.streamingtopic}")
    private String targetTopicSrc2;
    @Value("${mapr.ss3.streamingtopic}")
    private String targetTopicSwtchSrc3;
    @Value("${mapr.ss4.streamingtopic}")
    private String targetTopicSrc4;
    @Value("${mapr.ss5.streamingtopic}")
    private String targetTopicSrc5;

    @Value("${table.ss1.destination}")
    private String tableDestinationSrc1;
    @Value("${table.ss2.destination}")
    private String tableDestinationSrc2;
    @Value("${table.ss3.destination}")
    private String tableDestinationSrc3;
    @Value("${table.ss4.destination}")
    private String tableDestinationSrc4;
    @Value("${table.ss5.destination}")
    private String tableDestinationSrc5;

    @Value("${spring.ss1.datasource.url}")
    private String tableSourceSrc1;
    @Value("${spring.ss2.datasource.url}")
    private String tableSourceSrc2;
    @Value("${spring.ss3.datasource.url}")
    private String tableSourceSrc3;
    @Value("${spring.ss4.datasource.url}")
    private String tableSourceSrc4;
    @Value("${spring.ss5.datasource.url}")
    private String tableSourceSrc5;

    @Value("${jdbc.ss1.identityfieldname}")
    private String identityFieldNameSrc1;


    //setting other properties
    Map<String, String> props;

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    public Connection connection(){
        String connectionUrl = "ojai:mapr:";
        Connection connection = DriverManager.getConnection(connectionUrl);
        return connection;
    }

    @Bean
    public SparkConf sparkConf(){
        SparkConf conf = new SparkConf()
                .setAppName("spark-jdbc")
                .setIfMissing("spark.ui.enabled", "false")
                .setMaster("local[*]");
        return conf;
    }

    @Bean
    public static PropertiesFactoryBean mapper() throws IOException {
        PropertiesFactoryBean factoryBean = new PropertiesFactoryBean();
        factoryBean.setLocation(new ClassPathResource("application.properties"));
        factoryBean.afterPropertiesSet();
        return factoryBean;
    }

//    @Bean
//    public static KafkaProducer kafkaProducer(){
//        try {
//            Properties properties = mapper().getObject();
//            KafkaProducer<String, Object> producer =
//                    new KafkaProducer<>(properties);
//            return producer;
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
    @Bean
    public MapRDBJavaSession getMaprDBJavaSession() {
        return new MapRDBJavaSession(SparkSession.builder()
                .appName("spark-jdbc")
                .master("local[*]")
                .getOrCreate());
    }
    @Bean
    public SparkContext SparkContextBuilder()
    {
        SparkConf sparkConf = new SparkConf()
                .setMaster(props.get("spark.master"))
                .setAppName(props.get("spark.app.name"))
                .set("spark.driver.allowMultipleContexts", "true");

        SparkContext sparkContext = SparkContext.getOrCreate(sparkConf);
        return  (sparkContext) ;
    }

    @Bean
    public SparkSession SparkSessionBuilder()
    {
        return new SparkSession(SparkContextBuilder());
    }

    @Bean
    public SQLContext SqlContextBuilder()
    {
        return SQLContext.getOrCreate(SparkContextBuilder());
    }

    @Bean
    public Logger logger(){
        Logger logger = LoggerFactory.getLogger(SparkJobApplication.class);
        return logger;
    }

    public Map<String, String> getProps () {
        return props;
    }
    public void setProps(Map<String, String> props) {
        this.props = props;
    }

}