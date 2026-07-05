package io.github.abbas1123.analytics.model;

import java.math.BigDecimal;
import java.time.Instant;

public record FraudAlert(
        String accountId,
        Instant windowStart,
        long txCount,
        BigDecimal totalAmount,
        String reason,
        Instant detectedAt) {

    public static final String REASON_VELOCITY = "VELOCITY";
    public static final String REASON_AMOUNT = "AMOUNT";
}
