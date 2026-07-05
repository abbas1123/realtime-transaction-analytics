package io.github.abbas1123.analytics.stream;

import io.github.abbas1123.analytics.config.AnalyticsProperties;
import io.github.abbas1123.analytics.model.AccountWindowStats;
import io.github.abbas1123.analytics.model.FraudAlert;
import io.github.abbas1123.analytics.model.TransactionEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;

/**
 * transaction-events ──► group by source account ──► 1-minute tumbling window
 * ──► count + sum ──► threshold filter ──► fraud-alerts
 */
@Component
public class AnalyticsTopology {

    public static final String INPUT_TOPIC = "transaction-events";
    public static final String ALERTS_TOPIC = "fraud-alerts";
    public static final String STATS_STORE = "account-window-stats";

    private final AnalyticsProperties properties;

    public AnalyticsTopology(AnalyticsProperties properties) {
        this.properties = properties;
    }

    @Autowired
    public void buildPipeline(StreamsBuilder builder) {
        JsonSerde<TransactionEvent> eventSerde = jsonSerde(TransactionEvent.class);
        JsonSerde<AccountWindowStats> statsSerde = jsonSerde(AccountWindowStats.class);
        JsonSerde<FraudAlert> alertSerde = jsonSerde(FraudAlert.class);

        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), eventSerde))
                .filter((key, event) -> event != null && event.fromAccountId() != null)
                .groupBy((key, event) -> String.valueOf(event.fromAccountId()),
                        Grouped.with(Serdes.String(), eventSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(properties.window()))
                .aggregate(AccountWindowStats::empty,
                        (accountId, event, stats) -> stats.add(event),
                        Materialized.<String, AccountWindowStats, WindowStore<Bytes, byte[]>>as(STATS_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(statsSerde))
                .toStream()
                .filter((windowedKey, stats) -> stats != null && isSuspicious(stats))
                .map((windowedKey, stats) -> KeyValue.pair(windowedKey.key(), toAlert(windowedKey, stats)))
                .to(ALERTS_TOPIC, Produced.with(Serdes.String(), alertSerde));
    }

    private boolean isSuspicious(AccountWindowStats stats) {
        return stats.txCount() >= properties.velocityThreshold()
                || stats.totalAmount().compareTo(properties.amountThreshold()) >= 0;
    }

    private FraudAlert toAlert(Windowed<String> windowedKey, AccountWindowStats stats) {
        String reason = stats.txCount() >= properties.velocityThreshold()
                ? FraudAlert.REASON_VELOCITY
                : FraudAlert.REASON_AMOUNT;

        return new FraudAlert(
                windowedKey.key(),
                Instant.ofEpochMilli(windowedKey.window().start()),
                stats.txCount(),
                stats.totalAmount(),
                reason,
                Instant.now());
    }

    private static <T> JsonSerde<T> jsonSerde(Class<T> type) {
        JsonSerde<T> serde = new JsonSerde<>(type);
        serde.configure(Map.of(JsonDeserializer.TRUSTED_PACKAGES, "*"), false);
        return serde;
    }
}
