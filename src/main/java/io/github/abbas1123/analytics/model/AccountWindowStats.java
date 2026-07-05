package io.github.abbas1123.analytics.model;

import java.math.BigDecimal;

/**
 * Rolling per-account aggregate within one tumbling window.
 */
public record AccountWindowStats(long txCount, BigDecimal totalAmount) {

    public static AccountWindowStats empty() {
        return new AccountWindowStats(0L, BigDecimal.ZERO);
    }

    public AccountWindowStats add(TransactionEvent event) {
        BigDecimal amount = event.amount() == null ? BigDecimal.ZERO : event.amount();
        return new AccountWindowStats(txCount + 1, totalAmount.add(amount));
    }
}
