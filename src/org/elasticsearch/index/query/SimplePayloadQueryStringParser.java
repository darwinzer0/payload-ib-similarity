package org.elasticsearch.index.query;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;

public class SimplePayloadQueryStringParser implements QueryParser {

    public static final String NAME = "simple_payload_query_string";

    @Inject
    public SimplePayloadQueryStringParser(Settings settings) {

    }

    @Override
    public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        String currentFieldName = null;
        String queryBody = null;
        String field = null;
        Map<String, Float> fieldsAndWeights = null;
        BooleanClause.Occur defaultOperator = null;
        Analyzer analyzer = null;
        int flags = -1;
        SimpleQueryParser.Settings sqsSettings = new SimpleQueryParser.Settings();

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("fields".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String fField = null;
                        float fBoost = 1;
                        char[] text = parser.textCharacters();
                        int end = parser.textOffset() + parser.textLength();
                        for (int i = parser.textOffset(); i < end; i++) {
                            if (text[i] == '^') {
                                int relativeLocation = i - parser.textOffset();
                                fField = new String(text, parser.textOffset(), relativeLocation);
                                fBoost = Float.parseFloat(new String(text, i + 1, parser.textLength() - relativeLocation - 1));
                                break;
                            }
                        }
                        if (fField == null) {
                            fField = parser.text();
                        }

                        if (fieldsAndWeights == null) {
                            fieldsAndWeights = new HashMap<>();
                        }

                        if (Regex.isSimpleMatchPattern(fField)) {
                            for (String fieldName : parseContext.mapperService().simpleMatchToIndexNames(fField)) {
                                fieldsAndWeights.put(fieldName, fBoost);
                            }
                        } else {
                            MapperService.SmartNameFieldMappers mappers = parseContext.smartFieldMappers(fField);
                            if (mappers != null && mappers.hasMapper()) {
                                fieldsAndWeights.put(mappers.mapper().names().indexName(), fBoost);
                            } else {
                                fieldsAndWeights.put(fField, fBoost);
                            }
                        }
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(),
                            "[" + NAME + "] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("query".equals(currentFieldName)) {
                    queryBody = parser.text();
                } else if ("analyzer".equals(currentFieldName)) {
                    analyzer = parseContext.analysisService().analyzer(parser.text());
                    if (analyzer == null) {
                        throw new QueryParsingException(parseContext.index(), "[" + NAME + "] analyzer [" + parser.text() + "] not found");
                    }
                } else if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if ("default_operator".equals(currentFieldName) || "defaultOperator".equals(currentFieldName)) {
                    String op = parser.text();
                    if ("or".equalsIgnoreCase(op)) {
                        defaultOperator = BooleanClause.Occur.SHOULD;
                    } else if ("and".equalsIgnoreCase(op)) {
                        defaultOperator = BooleanClause.Occur.MUST;
                    } else {
                        throw new QueryParsingException(parseContext.index(),
                                "[" + NAME + "] default operator [" + op + "] is not allowed");
                    }
                } else if ("flags".equals(currentFieldName)) {
                    if (parser.hasTextCharacters()) {
                        // Possible options are:
                        // ALL, NONE, AND, OR, PREFIX, PHRASE, PRECEDENCE, ESCAPE, WHITESPACE, FUZZY, NEAR, SLOP
                        flags = SimplePayloadQueryStringFlag.resolveFlags(parser.text());
                    } else {
                        flags = parser.intValue();
                        if (flags < 0) {
                            flags = SimplePayloadQueryStringFlag.ALL.value();
                        }
                    }
                } else if ("locale".equals(currentFieldName)) {
                    String localeStr = parser.text();
                    Locale locale = LocaleUtils.parse(localeStr);
                    sqsSettings.locale(locale);
                } else if ("lowercase_expanded_terms".equals(currentFieldName)) {
                    sqsSettings.lowercaseExpandedTerms(parser.booleanValue());
                } else if ("lenient".equals(currentFieldName)) {
                    sqsSettings.lenient(parser.booleanValue());
                } else {
                    throw new QueryParsingException(parseContext.index(), "[" + NAME + "] unsupported field [" + parser.currentName() + "]");
                }
            }
        }

        // Query text is required
        if (queryBody == null) {
            throw new QueryParsingException(parseContext.index(), "[" + NAME + "] query text missing");
        }

        // Support specifying only a field instead of a map
        if (field == null) {
            field = currentFieldName;
        }

        // Use the default field (_all) if no fields specified
        if (fieldsAndWeights == null) {
            field = parseContext.defaultField();
        }

        // Use standard analyzer by default
        if (analyzer == null) {
            analyzer = parseContext.mapperService().searchAnalyzer();
        }

        if (fieldsAndWeights == null) {
            fieldsAndWeights = Collections.singletonMap(field, 1.0F);
        }
        SimplePayloadQueryParser sqp = new SimplePayloadQueryParser(analyzer, fieldsAndWeights, flags, sqsSettings);

        if (defaultOperator != null) {
            sqp.setDefaultOperator(defaultOperator);
        }

        return sqp.parse(queryBody);
    }
}
