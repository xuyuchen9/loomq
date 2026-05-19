package com.loomq.config;

import java.security.MessageDigest;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * HTTP API token authentication configuration.
 */
public record SecurityConfig(boolean enabled, String tokenHeader, Set<String> tokens) {

    private static final String DEFAULT_TOKEN_HEADER = "X-LoomQ-Token";

    public SecurityConfig {
        tokenHeader = requireText(tokenHeader, "tokenHeader");
        Set<String> cleaned = new LinkedHashSet<>();
        if (tokens != null) {
            for (String token : tokens) {
                if (token != null && !token.isBlank()) {
                    cleaned.add(token.trim());
                }
            }
        }
        tokens = Collections.unmodifiableSet(cleaned);
        if (enabled && tokens.isEmpty()) {
            throw new IllegalArgumentException("security.enabled=true requires at least one non-blank token");
        }
    }

    public static SecurityConfig disabled() {
        return new SecurityConfig(false, DEFAULT_TOKEN_HEADER, Set.of());
    }

    public static SecurityConfig fromProperties(Properties props) {
        Properties source = props == null ? new Properties() : props;
        boolean enabled = ConfigSupport.booleanValue(source, false, "security.enabled", "loomq.security.enabled");
        String tokenHeader = ConfigSupport.string(
            source,
            DEFAULT_TOKEN_HEADER,
            "security.token_header",
            "security.tokenHeader",
            "loomq.security.tokenHeader"
        );
        String tokenSpec = ConfigSupport.firstNonBlank(
            source,
            "security.tokens",
            "security.token",
            "loomq.security.tokens",
            "loomq.security.token"
        );
        return new SecurityConfig(enabled, tokenHeader, parseTokens(tokenSpec));
    }

    public boolean requiresAuthentication(String uri) {
        if (!enabled) {
            return false;
        }
        String path = pathOnly(uri);
        return !(path.equals("/health")
            || path.equals("/health/live")
            || path.equals("/health/ready")
            || path.equals("/health/deep"));
    }

    public boolean isAuthorized(String uri, Map<String, String> headers) {
        if (!requiresAuthentication(uri)) {
            return true;
        }
        String presented = header(headers, tokenHeader);
        if (presented == null || presented.isBlank()) {
            return false;
        }
        String normalized = normalizePresentedToken(presented);
        for (String token : tokens) {
            if (constantTimeEquals(normalized, token)) {
                return true;
            }
        }
        return false;
    }

    private static Set<String> parseTokens(String tokenSpec) {
        if (tokenSpec == null || tokenSpec.isBlank()) {
            return Set.of();
        }
        Set<String> parsed = new LinkedHashSet<>();
        for (String token : tokenSpec.split(",")) {
            if (token != null && !token.isBlank()) {
                parsed.add(token.trim());
            }
        }
        return parsed;
    }

    private static String normalizePresentedToken(String value) {
        String trimmed = value.trim();
        if (trimmed.regionMatches(true, 0, "Bearer ", 0, "Bearer ".length())) {
            return trimmed.substring("Bearer ".length()).trim();
        }
        return trimmed;
    }

    private static String header(Map<String, String> headers, String name) {
        if (headers == null || headers.isEmpty()) {
            return null;
        }
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(name)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static String pathOnly(String uri) {
        if (uri == null || uri.isBlank()) {
            return "";
        }
        int query = uri.indexOf('?');
        return query >= 0 ? uri.substring(0, query) : uri;
    }

    private static boolean constantTimeEquals(String left, String right) {
        return MessageDigest.isEqual(left.getBytes(java.nio.charset.StandardCharsets.UTF_8),
            right.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return value.trim();
    }
}
