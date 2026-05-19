package com.loomq.application.scheduler;

import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.Reliability;

/**
 * Maps SLO declarations to precision tier recommendations.
 *
 * Uses a 2× safety margin: the tier's precision window must be at most
 * half the user's maxTardinessMs. Tiers are evaluated from cheapest
 * (largest window) to most expensive (smallest window), so the first
 * match is the most cost-effective.
 */
public final class TierAdvisor {

    private TierAdvisor() {}

    public record Recommendation(PrecisionTier tier, String rationale) {}

    private static final PrecisionTier[] TIERS_CHEAPEST_FIRST = {
        PrecisionTier.ECONOMY, PrecisionTier.STANDARD,
        PrecisionTier.HIGH, PrecisionTier.FAST, PrecisionTier.ULTRA
    };

    public static Recommendation recommend(long maxTardinessMs, Reliability reliability) {
        if (maxTardinessMs <= 0) {
            throw new IllegalArgumentException("maxTardinessMs must be positive, got: " + maxTardinessMs);
        }
        if (reliability == null) {
            throw new IllegalArgumentException("reliability must not be null");
        }
        PrecisionTierCatalog catalog = PrecisionTierCatalog.defaultCatalog();
        int safetyFactor = reliability == Reliability.AT_LEAST_ONCE ? 2 : 1;
        long safetyWindowMs = maxTardinessMs / safetyFactor;

        for (PrecisionTier tier : TIERS_CHEAPEST_FIRST) {
            if (catalog.precisionWindowMs(tier) <= safetyWindowMs) {
                return new Recommendation(tier,
                    buildRationale(tier, catalog, maxTardinessMs, safetyFactor, reliability));
            }
        }

        PrecisionTier fallback = PrecisionTier.ULTRA;
        return new Recommendation(fallback,
            String.format(
                "ULTRA tier (%dms precision) is the tightest available but does not meet "
                    + "maxTardinessMs=%dms with %d× safety margin. "
                    + "Consider relaxing the SLO or accepting best-effort latency.",
                catalog.precisionWindowMs(fallback), maxTardinessMs, safetyFactor));
    }

    private static String buildRationale(PrecisionTier tier, PrecisionTierCatalog catalog,
                                          long maxTardinessMs, int safetyFactor,
                                          Reliability reliability) {
        double margin = maxTardinessMs / (double) catalog.precisionWindowMs(tier);
        return String.format(
            "%s tier (%dms precision, %s batching, %d max concurrency) satisfies "
                + "maxTardinessMs=%dms with %.0f× safety margin (%s).",
            tier.name(),
            catalog.precisionWindowMs(tier),
            catalog.isBatchEnabled(tier) ? "batched" : "no",
            catalog.maxConcurrency(tier),
            maxTardinessMs,
            margin,
            reliability.name());
    }
}
