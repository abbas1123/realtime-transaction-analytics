package io.github.abbas1123.analytics.stream;

import io.github.abbas1123.analytics.config.AnalyticsProperties;
import io.github.abbas1123.analytics.model.FraudAlert;
import io.github.abbas1123.analytics.model.TransactionEvent;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class AnalyticsTopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, TransactionEvent> inputTopic;
    private TestOutputTopic<String, FraudAlert> alertsTopic;

    @BeforeEach
    void setUp() {
        AnalyticsProperties properties = new AnalyticsProperties(
                Duration.ofMinutes(1), 5L, new BigDecimal("1000"));

        StreamsBuilder builder = new StreamsBuilder();
        new AnalyticsTopology(properties).buildPipeline(builder);

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        driver = new TopologyTestDriver(builder.build(), config);

        inputTopic = driver.createInputTopic(
                AnalyticsTopology.INPUT_TOPIC, new StringSerializer(), new JsonSerializer<>());

        JsonDeserializer<FraudAlert> alertDeserializer = new JsonDeserializer<>(FraudAlert.class);
        alertDeserializer.addTrustedPackages("*");
        alertsTopic = driver.createOutputTopic(
                AnalyticsTopology.ALERTS_TOPIC, new StringDeserializer(), alertDeserializer);
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void raisesVelocityAlertOnFifthTransferInWindow() {
        Instant base = Instant.ofEpochMilli(0);
        for (int i = 0; i < 5; i++) {
            inputTopic.pipeInput("k", event(7L, "10.00", base.plusSeconds(i * 10L)), base.plusSeconds(i * 10L));
        }

        List<FraudAlert> alerts = alertsTopic.readValuesToList();

        assertThat(alerts).hasSize(1);
        FraudAlert alert = alerts.get(0);
        assertThat(alert.accountId()).isEqualTo("7");
        assertThat(alert.reason()).isEqualTo(FraudAlert.REASON_VELOCITY);
        assertThat(alert.txCount()).isEqualTo(5);
        assertThat(alert.windowStart()).isEqualTo(Instant.ofEpochMilli(0));
    }

    @Test
    void raisesAmountAlertWhenWindowTotalCrossesThreshold() {
        Instant base = Instant.ofEpochMilli(0);
        inputTopic.pipeInput("k", event(8L, "600.00", base), base);
        inputTopic.pipeInput("k", event(8L, "450.00", base.plusSeconds(20)), base.plusSeconds(20));

        List<FraudAlert> alerts = alertsTopic.readValuesToList();

        assertThat(alerts).hasSize(1);
        FraudAlert alert = alerts.get(0);
        assertThat(alert.reason()).isEqualTo(FraudAlert.REASON_AMOUNT);
        assertThat(alert.totalAmount()).isEqualByComparingTo("1050.00");
    }

    @Test
    void staysQuietBelowThresholds() {
        Instant base = Instant.ofEpochMilli(0);
        inputTopic.pipeInput("k", event(9L, "10.00", base), base);
        inputTopic.pipeInput("k", event(9L, "20.00", base.plusSeconds(5)), base.plusSeconds(5));
        inputTopic.pipeInput("k", event(9L, "30.00", base.plusSeconds(10)), base.plusSeconds(10));

        assertThat(alertsTopic.readValuesToList()).isEmpty();
    }

    @Test
    void separateWindowsAreCountedIndependently() {
        Instant firstWindow = Instant.ofEpochMilli(0);
        Instant secondWindow = firstWindow.plus(Duration.ofMinutes(2));

        for (int i = 0; i < 4; i++) {
            inputTopic.pipeInput("k", event(10L, "10.00", firstWindow.plusSeconds(i)), firstWindow.plusSeconds(i));
        }
        for (int i = 0; i < 4; i++) {
            inputTopic.pipeInput("k", event(10L, "10.00", secondWindow.plusSeconds(i)), secondWindow.plusSeconds(i));
        }

        assertThat(alertsTopic.readValuesToList()).isEmpty();
    }

    private static TransactionEvent event(Long fromAccount, String amount, Instant at) {
        return new TransactionEvent(1L, fromAccount, 2L, new BigDecimal(amount),
                new BigDecimal("0.10"), "COMPLETED", at);
    }
}
