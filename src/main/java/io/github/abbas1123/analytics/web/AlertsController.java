package io.github.abbas1123.analytics.web;

import io.github.abbas1123.analytics.model.FraudAlert;
import io.github.abbas1123.analytics.service.AlertStore;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/alerts")
@Tag(name = "Fraud Alerts", description = "Most recent alerts produced by the streaming topology")
public class AlertsController {

    private final AlertStore alertStore;

    public AlertsController(AlertStore alertStore) {
        this.alertStore = alertStore;
    }

    @GetMapping
    @Operation(summary = "List recent fraud alerts, optionally filtered by account")
    public List<FraudAlert> recentAlerts(
            @RequestParam(required = false) String accountId,
            @RequestParam(defaultValue = "50") int limit) {
        return alertStore.recent(accountId, Math.min(limit, 200));
    }
}
