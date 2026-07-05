package io.github.abbas1123.analytics.config;

import io.github.abbas1123.analytics.stream.AnalyticsTopology;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@EnableKafkaStreams
public class KafkaConfig {

    @Bean
    public NewTopic transactionEventsTopic() {
        return TopicBuilder.name(AnalyticsTopology.INPUT_TOPIC).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic fraudAlertsTopic() {
        return TopicBuilder.name(AnalyticsTopology.ALERTS_TOPIC).partitions(3).replicas(1).build();
    }
}
