package com.bigdata_project.demo;

import com.bigdata_project.demo.jobs.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
@EnableConfigurationProperties({AppConfig.class})
public class SparkJobApplication  implements CommandLineRunner {

    @Autowired
    SparkJobSource1 sparkJobSource1;

    @Autowired
    SparkJobSource2 sparkJobSource2;

    @Autowired
    SparkJobSource3 sparkJobSource3;

    @Autowired
    SparkJobSource4 sparkJobSource4;

    @Autowired
    SparkJobSource5 sparkJobSource5;

    public static void main(String[] args) {
        SpringApplication.run(SparkJobApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception    {
////        Set<Service> services = new HashSet<>();
//        Set<Service> services = new HashSet<>();
//        services.add(sparkJobsr1);
//        services.add(sparkJobsr2);
//        ServiceManager manager = new ServiceManager(services);
//        manager
//
        //runs the jobs concurrently
        sparkJobSource1.start();
        sparkJobSource2.start();
        sparkJobSource3.start();
        sparkJobSource4.start();
        sparkJobSource5.start();

    }

}
