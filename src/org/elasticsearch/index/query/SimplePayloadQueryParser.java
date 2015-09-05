package org.elasticsearch.index.query;

import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.payloads.AveragePayloadFunction;
import org.apache.lucene.search.payloads.PayloadNearQuery;
import org.apache.lucene.search.payloads.PayloadTermQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.BooleanClause;

/**
 * SimpleQueryParser is used to parse human readable query syntax.
 * 
 * The main idea behind this parser is that a person should be able to type
 * whatever they want to represent a query, and this parser will do its best
 * to interpret what to search for no matter how poorly composed the request
 * may be. Tokens are considered to be any of a term, phrase, or subquery for the
 * operations described below.  Whitespace including ' ' '\n' '\r' and '\t'
 * and certain operators may be used to delimit tokens ( ) + | " .
 * 
 * Any errors in query syntax will be ignored and the parser will attempt
 * to decipher what it can; however, this may mean odd or unexpected results.
 * Query Operators
 * 
 *  '{@code +}' specifies {@code AND} operation: token1+token2
 *  '{@code |}' specifies {@code OR} operation: token1|token2
 *  '{@code -}' negates a single token: -token0
 *  '{@code "}' creates phrases of terms: "term1 term2 ..."
 *  '{@code *}' at the end of terms specifies prefix query: term*
 *  '{@code ~}N' at the end of terms specifies fuzzy query: term~1
 *  '{@code ~}N' at the end of phrases specifies near query: "term1 term2"~5
 *  '{@code (}' and '{@code )}' specifies precedence: token1 + (token2 | token3)
 * 
 * 
 * The {@link #setDefaultOperator default operator} is {@code OR} if no other operator is specified.
 * For example, the following will {@code OR} {@code token1} and {@code token2} together:
 * token1 token2
 * 
 * Normal operator precedence will be simple order from right to left.
 * For example, the following will evaluate {@code token1 OR token2} first,
 * then {@code AND} with {@code token3}:
 * token1 | token2 + token3
 * Escaping
 * 
 * An individual term may contain any possible character with certain characters
 * requiring escaping using a '{@code \}'.  The following characters will need to be escaped in
 * terms and phrases:
 * {@code + | " ( ) ' \}
 * 
 * The '{@code -}' operator is a special case.  On individual terms (not phrases) the first
 * character of a term that is {@code -} must be escaped; however, any '{@code -}' characters
 * beyond the first character do not need to be escaped.
 * For example:
 * 
 *   {@code -term1}   -- Specifies {@code NOT} operation against {@code term1}
 *   {@code \-term1}  -- Searches for the term {@code -term1}.
 *   {@code term-1}   -- Searches for the term {@code term-1}.
 *   {@code term\-1}  -- Searches for the term {@code term-1}.
 * 
 * 
 * The '{@code *}' operator is a special case. On individual terms (not phrases) the last
 * character of a term that is '{@code *}' must be escaped; however, any '{@code *}' characters
 * before the last character do not need to be escaped:
 * 
 *   {@code term1*}  --  Searches for the prefix {@code term1}
 *   {@code term1\*} --  Searches for the term {@code term1*}
 *   {@code term*1}  --  Searches for the term {@code term*1}
 *   {@code term\*1} --  Searches for the term {@code term*1}
 * 
 * 
 * Note that above examples consider the terms before text processing.
 */
public class SimplePayloadQueryParser extends SimpleQueryParser {

    
    public SimplePayloadQueryParser(Analyzer analyzer, Map<String, Float> weights, int flags, Settings settings) {
        super(analyzer, weights, flags, settings);
    }
    
    @Override
    protected Query newTermQuery(Term term) {
        return new PayloadTermQuery(term, new AveragePayloadFunction(), true);
    }

	/**
 	* Factory method to generate a phrase query with slop.
	*/
    @Override
    public Query newPhraseQuery(String text, int slop) {
		BooleanQuery bq = new BooleanQuery(true);
		for (Map.Entry<String,Float> entry : weights.entrySet()) {
			Query q = createPhraseQuery(entry.getKey(), text, slop); // entry.getKey() is the FIELD
			if (q instanceof PhraseQuery) {
				PhraseQuery pq = (PhraseQuery) q; 
				Term[] terms = pq.getTerms(); 
				SpanQuery[] clauses = new SpanQuery[terms.length];
				for (int i = 0; i < terms.length; i++) 
					clauses[i] = new PayloadTermQuery(terms[i], new AveragePayloadFunction(), true); 
				q = new PayloadNearQuery(clauses, slop, true);  
			}
			if (q != null) {
				q.setBoost(entry.getValue());
				bq.add(q, BooleanClause.Occur.SHOULD);
			}
		}
		return simplify(bq);
	}
    
}
