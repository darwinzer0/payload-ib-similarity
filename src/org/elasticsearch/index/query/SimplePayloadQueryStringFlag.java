package org.elasticsearch.index.query;

import java.util.Locale;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Strings;

public enum SimplePayloadQueryStringFlag {
    ALL(-1),
    NONE(0),
    AND(SimplePayloadQueryParser.AND_OPERATOR),
    NOT(SimplePayloadQueryParser.NOT_OPERATOR),
    OR(SimplePayloadQueryParser.OR_OPERATOR),
    PREFIX(SimplePayloadQueryParser.PREFIX_OPERATOR),
    PHRASE(SimplePayloadQueryParser.PHRASE_OPERATOR),
    PRECEDENCE(SimplePayloadQueryParser.PRECEDENCE_OPERATORS),
    ESCAPE(SimplePayloadQueryParser.ESCAPE_OPERATOR),
    WHITESPACE(SimplePayloadQueryParser.WHITESPACE_OPERATOR),
    FUZZY(SimplePayloadQueryParser.FUZZY_OPERATOR),
    // NEAR and SLOP are synonymous, since "slop" is a more familiar term than "near"
    NEAR(SimplePayloadQueryParser.NEAR_OPERATOR),
    SLOP(SimplePayloadQueryParser.NEAR_OPERATOR);

    final int value;

    private SimplePayloadQueryStringFlag(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    static int resolveFlags(String flags) {
        if (!Strings.hasLength(flags)) {
            return ALL.value();
        }
        int magic = NONE.value();
        for (String s : Strings.delimitedListToStringArray(flags, "|")) {
            if (s.isEmpty()) {
                continue;
            }
            try {
                SimplePayloadQueryStringFlag flag = SimplePayloadQueryStringFlag.valueOf(s.toUpperCase(Locale.ROOT));
                switch (flag) {
                    case NONE:
                        return 0;
                    case ALL:
                        return -1;
                    default:
                        magic |= flag.value();
                }
            } catch (IllegalArgumentException iae) {
                throw new ElasticsearchIllegalArgumentException("Unknown " + SimplePayloadQueryStringParser.NAME + " flag [" + s + "]");
            }
        }
        return magic;
    }
}
