package com.fronzec.logagregator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;

@SpringBootApplication
@EnableBatchProcessing
public class LogAgregatorServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(LogAgregatorServiceApplication.class, args);
    }
}
