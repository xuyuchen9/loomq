package com.loomq.config;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Lightweight YAML loader for LoomQ configuration files.
 *
 * <p>This loader intentionally supports only the YAML subset used by the
 * bundled configuration files:
 * <ul>
 *   <li>nested mappings with 2-space indentation</li>
 *   <li>simple scalar lists</li>
 *   <li>inline scalar lists such as {@code [1, 2, 3]}</li>
 *   <li>quoted and unquoted scalar values</li>
 *   <li>`${ENV:default}` placeholder expansion</li>
 * </ul>
 *
 * <p>The parsed result is flattened into dot-separated properties so the
 * existing config records can continue to use {@link Properties}.
 */
public final class SimpleYamlConfigLoader {

    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("^\\$\\{([^:}]+)(?::([^}]*))?}$");

    private SimpleYamlConfigLoader() {
    }

    public static Properties load(InputStream input) throws IOException {
        Objects.requireNonNull(input, "input cannot be null");

        List<Line> lines = readLines(input);
        Properties properties = new Properties();
        parseBlock(lines, 0, 0, "", properties);
        return properties;
    }

    public static Properties load(String yaml) {
        Objects.requireNonNull(yaml, "yaml cannot be null");

        try (InputStream input = new java.io.ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8))) {
            return load(input);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to parse YAML content", e);
        }
    }

    private static List<Line> readLines(InputStream input) throws IOException {
        List<Line> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
            String raw;
            int lineNumber = 0;
            while ((raw = reader.readLine()) != null) {
                lineNumber++;
                String stripped = stripComment(raw).stripTrailing();
                if (stripped.isBlank()) {
                    continue;
                }

                int indent = countLeadingSpaces(stripped);
                if (indent % 2 != 0) {
                    throw new IllegalArgumentException("Invalid indentation at line " + lineNumber);
                }

                lines.add(new Line(lineNumber, indent, stripped.substring(indent).trim()));
            }
        }
        return lines;
    }

    private static int parseBlock(List<Line> lines, int index, int indent, String prefix, Properties target) {
        while (index < lines.size()) {
            Line line = lines.get(index);
            if (line.indent() < indent) {
                return index;
            }
            if (line.indent() > indent) {
                throw new IllegalArgumentException("Unexpected indentation at line " + line.number());
            }

            if (line.content().startsWith("-")) {
                throw new IllegalArgumentException("Unexpected list item at line " + line.number());
            }

            int colonIndex = line.content().indexOf(':');
            if (colonIndex < 0) {
                throw new IllegalArgumentException("Expected key:value pair at line " + line.number());
            }

            String key = line.content().substring(0, colonIndex).trim();
            String value = line.content().substring(colonIndex + 1).trim();
            if (key.isEmpty()) {
                throw new IllegalArgumentException("Empty key at line " + line.number());
            }

            String fullKey = prefix.isEmpty() ? key : prefix + "." + key;
            if (!value.isEmpty()) {
                target.setProperty(fullKey, parseScalar(value));
                index++;
                continue;
            }

            if (index + 1 >= lines.size()) {
                target.setProperty(fullKey, "");
                return index + 1;
            }

            Line next = lines.get(index + 1);
            if (next.indent() <= indent) {
                target.setProperty(fullKey, "");
                index++;
                continue;
            }

            if (next.content().startsWith("-")) {
                index = parseList(lines, index + 1, indent + 2, fullKey, target);
            } else {
                index = parseBlock(lines, index + 1, indent + 2, fullKey, target);
            }
        }
        return index;
    }

    private static int parseList(List<Line> lines, int index, int indent, String key, Properties target) {
        List<String> values = new ArrayList<>();
        while (index < lines.size()) {
            Line line = lines.get(index);
            if (line.indent() < indent) {
                break;
            }
            if (line.indent() > indent) {
                throw new IllegalArgumentException("Unexpected indentation in list at line " + line.number());
            }
            if (!line.content().startsWith("-")) {
                break;
            }

            String item = line.content().substring(1).trim();
            if (item.isEmpty()) {
                throw new IllegalArgumentException("Empty list item at line " + line.number());
            }
            values.add(parseScalar(item));
            index++;
        }

        target.setProperty(key, String.join(",", values));
        return index;
    }

    private static String parseScalar(String rawValue) {
        String value = unquote(rawValue.trim());
        if (isInlineList(value)) {
            return String.join(",", parseInlineList(value));
        }
        return resolvePlaceholders(value);
    }

    private static boolean isInlineList(String value) {
        return value.length() >= 2 && value.startsWith("[") && value.endsWith("]");
    }

    private static List<String> parseInlineList(String value) {
        String body = value.substring(1, value.length() - 1).trim();
        List<String> values = new ArrayList<>();
        if (body.isBlank()) {
            return values;
        }

        StringBuilder current = new StringBuilder();
        boolean inSingle = false;
        boolean inDouble = false;

        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '\\' && i + 1 < body.length()) {
                current.append(c).append(body.charAt(++i));
                continue;
            }
            if (c == '\'' && !inDouble) {
                inSingle = !inSingle;
                current.append(c);
                continue;
            }
            if (c == '"' && !inSingle) {
                inDouble = !inDouble;
                current.append(c);
                continue;
            }
            if (c == ',' && !inSingle && !inDouble) {
                values.add(resolvePlaceholders(unquote(current.toString().trim())));
                current.setLength(0);
                continue;
            }
            current.append(c);
        }

        if (current.length() > 0) {
            values.add(resolvePlaceholders(unquote(current.toString().trim())));
        }
        return values;
    }

    private static String resolvePlaceholders(String value) {
        if (value == null) {
            return null;
        }

        Matcher matcher = PLACEHOLDER_PATTERN.matcher(value.trim());
        if (!matcher.matches()) {
            return value;
        }

        String key = matcher.group(1).trim();
        String defaultValue = matcher.group(2);

        String resolved = System.getProperty(key);
        if (resolved == null || resolved.isBlank()) {
            resolved = System.getenv(key);
        }
        if (resolved == null || resolved.isBlank()) {
            resolved = defaultValue;
        }
        return resolved != null ? resolved : "";
    }

    private static String stripComment(String line) {
        boolean inSingle = false;
        boolean inDouble = false;
        StringBuilder result = new StringBuilder(line.length());

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '\\' && i + 1 < line.length()) {
                result.append(c).append(line.charAt(++i));
                continue;
            }
            if (c == '\'' && !inDouble) {
                inSingle = !inSingle;
                result.append(c);
                continue;
            }
            if (c == '"' && !inSingle) {
                inDouble = !inDouble;
                result.append(c);
                continue;
            }
            if (c == '#' && !inSingle && !inDouble) {
                break;
            }
            result.append(c);
        }

        return result.toString();
    }

    private static int countLeadingSpaces(String value) {
        int count = 0;
        while (count < value.length() && value.charAt(count) == ' ') {
            count++;
        }
        return count;
    }

    private static String unquote(String value) {
        if (value.length() >= 2) {
            char first = value.charAt(0);
            char last = value.charAt(value.length() - 1);
            if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
                return value.substring(1, value.length() - 1);
            }
        }
        return value;
    }

    private record Line(int number, int indent, String content) {
    }
}
