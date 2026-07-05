package io.github.abbas1123.analytics.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Mirrors the event contract published by payment-transactions-api
 * (github.com/abbas1123/payment-transactions-api) to the
 * {@code transaction-events} topic.
 */
public record TransactionEvent(
        Long transactionId,
        Long fromAccountId,
        Long toAccountId,
        BigDecimal amount,
        BigDecimal commission,
        String status,
        Instant occurredAt) {
}
