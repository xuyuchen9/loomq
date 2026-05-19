package com.loomq.common;

import com.loomq.application.scheduler.CohortManager;
import com.loomq.application.scheduler.CohortManager.CohortWake;
import com.loomq.application.scheduler.PrecisionScheduler;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Assembles temporal forecast data from the scheduler internals.
 *
 * Surfaces the "time machine" view: what will the scheduler be doing
 * in the next N minutes? Which cohorts will wake? How many intents
 * are stacked behind them?
 */
public final class TimelineService {

    private TimelineService() {}

    public static Map<String, Object> build(CohortManager cohortManager,
                                             PrecisionScheduler scheduler,
                                             Instant from, Instant to) {
        Map<String, Object> root = new LinkedHashMap<>();

        List<Map<String, Object>> wakes = new ArrayList<>();
        for (CohortWake wake : cohortManager.getUpcomingWakes(from, to)) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("wakeAt", Instant.ofEpochMilli(wake.wakeAtMs()).toString());
            entry.put("intentCount", wake.intentCount());
            entry.put("tier", wake.tier());
            wakes.add(entry);
        }
        root.put("upcomingWakes", wakes);

        root.put("forecast", buildForecast(scheduler, from, to));

        return root;
    }

    private static Map<String, Object> buildForecast(PrecisionScheduler scheduler,
                                                      Instant from, Instant to) {
        Map<String, Object> forecast = new LinkedHashMap<>();
        var backpressure = scheduler.getBackpressureStatus();

        long windowMs = to.toEpochMilli() - from.toEpochMilli();
        long totalActive = backpressure.values().stream()
            .mapToLong(PrecisionScheduler.BackpressureInfo::activeDispatches)
            .sum();
        long totalCapacity = backpressure.values().stream()
            .mapToLong(PrecisionScheduler.BackpressureInfo::maxConcurrency)
            .sum();
        double utilPct = totalCapacity > 0 ? (totalActive * 100.0) / totalCapacity : 0;

        String risk;
        if (utilPct > 80) risk = "high";
        else if (utilPct > 50) risk = "medium";
        else risk = "low";

        // Find the tier with the most active dispatches
        String peakTier = "none";
        long peakActive = 0;
        for (var entry : backpressure.entrySet()) {
            long active = entry.getValue().activeDispatches();
            if (active > peakActive) {
                peakActive = active;
                peakTier = entry.getKey().name();
            }
        }

        forecast.put("peakLoadTier", peakTier);
        forecast.put("currentDispatchCount", totalActive);
        forecast.put("availableCapacity", totalCapacity - totalActive);
        forecast.put("backpressureRisk", risk);
        forecast.put("windowMs", windowMs);

        return forecast;
    }
}
