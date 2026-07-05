package io.github.abbas1123.analytics.service;

import io.github.abbas1123.analytics.model.FraudAlert;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Bounded in-memory buffer of the most recent alerts, backing the REST view.
 * Deliberately simple for the demo — a production deployment would query the
 * Kafka Streams state store interactively or sink alerts to a database.
 */
@Component
public class AlertStore {

    static final int CAPACITY = 200;

    private final Deque<FraudAlert> alerts = new ArrayDeque<>();

    public synchronized void add(FraudAlert alert) {
        alerts.addFirst(alert);
        while (alerts.size() > CAPACITY) {
            alerts.removeLast();
        }
    }

    public synchronized List<FraudAlert> recent(String accountId, int limit) {
        List<FraudAlert> result = new ArrayList<>();
        for (FraudAlert alert : alerts) {
            if (accountId == null || alert.accountId().equals(accountId)) {
                result.add(alert);
                if (result.size() >= limit) {
                    break;
                }
            }
        }
        return result;
    }
}
