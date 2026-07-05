package io.github.abbas1123.analytics.service;

import io.github.abbas1123.analytics.model.FraudAlert;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class AlertStoreTest {

    private final AlertStore store = new AlertStore();

    @Test
    void returnsNewestFirst() {
        store.add(alert("1", 5));
        store.add(alert("2", 6));

        assertThat(store.recent(null, 10))
                .extracting(FraudAlert::accountId)
                .containsExactly("2", "1");
    }

    @Test
    void filtersByAccountAndAppliesLimit() {
        store.add(alert("1", 5));
        store.add(alert("2", 6));
        store.add(alert("1", 7));
        store.add(alert("1", 8));

        assertThat(store.recent("1", 2))
                .hasSize(2)
                .extracting(FraudAlert::txCount)
                .containsExactly(8L, 7L);
    }

    @Test
    void evictsOldestBeyondCapacity() {
        for (int i = 0; i < AlertStore.CAPACITY + 25; i++) {
            store.add(alert(String.valueOf(i), i));
        }

        assertThat(store.recent(null, Integer.MAX_VALUE)).hasSize(AlertStore.CAPACITY);
        assertThat(store.recent("0", 10)).isEmpty();
    }

    private static FraudAlert alert(String accountId, long txCount) {
        return new FraudAlert(accountId, Instant.EPOCH, txCount,
                new BigDecimal("100.00"), FraudAlert.REASON_VELOCITY, Instant.now());
    }
}
