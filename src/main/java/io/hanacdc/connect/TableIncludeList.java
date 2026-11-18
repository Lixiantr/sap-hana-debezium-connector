package io.hanacdc.connect;

import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** Debezium-style anchored regular-expression selection for schema.table ids. */
public final class TableIncludeList {
    private final List<Pattern> patterns;

    private TableIncludeList(List<Pattern> patterns) {
        this.patterns = List.copyOf(patterns);
    }

    public static TableIncludeList compile(List<String> expressions) {
        if (expressions == null || expressions.isEmpty()) {
            throw new ConfigException(HanaSourceConnector.TABLE_INCLUDE_LIST
                    + " must contain at least one fully-qualified table expression");
        }
        List<Pattern> patterns = new ArrayList<>(expressions.size());
        for (String expression : expressions) {
            if (expression == null || expression.isBlank()) {
                throw new ConfigException(HanaSourceConnector.TABLE_INCLUDE_LIST
                        + " contains an empty expression");
            }
            try {
                patterns.add(Pattern.compile(expression));
            }
            catch (PatternSyntaxException error) {
                throw new ConfigException(HanaSourceConnector.TABLE_INCLUDE_LIST,
                        expression, "invalid regular expression: " + error.getMessage());
            }
        }
        return new TableIncludeList(patterns);
    }

    public boolean includes(String schema, String table) {
        String qualified = schema + "." + table;
        return patterns.stream().anyMatch(pattern -> pattern.matcher(qualified).matches());
    }
}
