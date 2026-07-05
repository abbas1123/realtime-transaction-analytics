package io.github.abbas1123.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class TransactionAnalyticsApplication {

    public static void main(String[] args) {
        SpringApplication.run(TransactionAnalyticsApplication.class, args);
    }
}
