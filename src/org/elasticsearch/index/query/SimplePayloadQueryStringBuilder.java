package org.elasticsearch.index.query;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class SimplePayloadQueryStringBuilder extends BaseQueryBuilder {
    private Map<String, Float> fields = new HashMap<>();
    private String analyzer;
    private Operator operator;
    private final String queryText;
    private int flags = -1;
    private Boolean lowercaseExpandedTerms;
    private Boolean lenient;
    private Locale locale;

    /**
     * Operators for the default_operator
     */
    public static enum Operator {
        AND,
        OR
    }

    /**
     * Construct a new simple query with the given text
     */
    public SimplePayloadQueryStringBuilder(String text) {
        this.queryText = text;
    }

    /**
     * Add a field to run the query against
     */
    public SimplePayloadQueryStringBuilder field(String field) {
        this.fields.put(field, null);
        return this;
    }

    /**
     * Add a field to run the query against with a specific boost
     */
    public SimplePayloadQueryStringBuilder field(String field, float boost) {
        this.fields.put(field, boost);
        return this;
    }

    /**
     * Specify an analyzer to use for the query
     */
    public SimplePayloadQueryStringBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /**
     * Specify the default operator for the query. Defaults to "OR" if no
     * operator is specified
     */
    public SimplePayloadQueryStringBuilder defaultOperator(Operator defaultOperator) {
        this.operator = defaultOperator;
        return this;
    }

    /**
     * Specify the enabled features of the SimpleQueryString.
     */
    public SimplePayloadQueryStringBuilder flags(SimplePayloadQueryStringFlag... flags) {
        int value = 0;
        if (flags.length == 0) {
            value = SimplePayloadQueryStringFlag.ALL.value;
        } else {
            for (SimplePayloadQueryStringFlag flag : flags) {
                value |= flag.value;
            }
        }
        this.flags = value;
        return this;
    }

    public SimplePayloadQueryStringBuilder lowercaseExpandedTerms(boolean lowercaseExpandedTerms) {
        this.lowercaseExpandedTerms = lowercaseExpandedTerms;
        return this;
    }

    public SimplePayloadQueryStringBuilder locale(Locale locale) {
        this.locale = locale;
        return this;
    }

    public SimplePayloadQueryStringBuilder lenient(boolean lenient) {
        this.lenient = lenient;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(SimplePayloadQueryStringParser.NAME);

        builder.field("query", queryText);

        if (fields.size() > 0) {
            builder.startArray("fields");
            for (Map.Entry<String, Float> entry : fields.entrySet()) {
                String field = entry.getKey();
                Float boost = entry.getValue();
                if (boost != null) {
                    builder.value(field + "^" + boost);
                } else {
                    builder.value(field);
                }
            }
            builder.endArray();
        }

        if (flags != -1) {
            builder.field("flags", flags);
        }

        if (analyzer != null) {
            builder.field("analyzer", analyzer);
        }

        if (operator != null) {
            builder.field("default_operator", operator.name().toLowerCase(Locale.ROOT));
        }

        if (lowercaseExpandedTerms != null) {
            builder.field("lowercase_expanded_terms", lowercaseExpandedTerms);
        }

        if (lenient != null) {
            builder.field("lenient", lenient);
        }

        if (locale != null) {
            builder.field("locale", locale.toString());
        }

        builder.endObject();
    }
}
