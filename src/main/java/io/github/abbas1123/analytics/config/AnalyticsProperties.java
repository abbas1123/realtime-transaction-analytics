package io.github.abbas1123.analytics.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * Detection thresholds, tunable without touching the topology.
 *
 * @param window            tumbling window size
 * @param velocityThreshold alert when an account sends this many transfers within one window
 * @param amountThreshold   alert when the windowed total reaches this amount
 */
@ConfigurationProperties(prefix = "analytics")
public record AnalyticsProperties(
        @DefaultValue("PT1M") Duration window,
        @DefaultValue("5") long velocityThreshold,
        @DefaultValue("1000") BigDecimal amountThreshold) {
}
