package com.loomq.api;

import com.loomq.domain.intent.Reliability;

/**
 * SLO (Service Level Objective) specification.
 *
 * Users declare their requirement ("I need delivery within 100ms")
 * instead of picking a precision tier manually. The system maps
 * the SLO to the most cost-effective tier that satisfies it.
 */
public record SloRequest(
    long maxTardinessMs,
    Reliability reliability
) {}
