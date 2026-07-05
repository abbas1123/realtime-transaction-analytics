package io.github.abbas1123.analytics.service;

import io.github.abbas1123.analytics.model.FraudAlert;
import io.github.abbas1123.analytics.stream.AnalyticsTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AlertsConsumer {

    private static final Logger log = LoggerFactory.getLogger(AlertsConsumer.class);

    private final AlertStore alertStore;

    public AlertsConsumer(AlertStore alertStore) {
        this.alertStore = alertStore;
    }

    @KafkaListener(topics = AnalyticsTopology.ALERTS_TOPIC, groupId = "alerts-dashboard")
    public void onAlert(FraudAlert alert) {
        log.warn("FRAUD ALERT [{}] account={} txCount={} total={} window={}",
                alert.reason(), alert.accountId(), alert.txCount(), alert.totalAmount(), alert.windowStart());
        alertStore.add(alert);
    }
}
