package org.apache.lucene.search.similarities;

import java.io.IOException;

import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.similarities.BasicStats;
import org.apache.lucene.search.similarities.Distribution;
import org.apache.lucene.search.similarities.IBSimilarity;
import org.apache.lucene.search.similarities.Lambda;
import org.apache.lucene.search.similarities.MultiSimilarity;
import org.apache.lucene.search.similarities.Normalization;
import org.apache.lucene.util.BytesRef;

public class ArticlePayloadIBSimilarity extends IBSimilarity {

    public ArticlePayloadIBSimilarity(Distribution distribution, Lambda lambda,
            Normalization normalization) {
        super(distribution, lambda, normalization);
    }

    public float scorePayload(int doc, int start, int end, BytesRef payload) {
        if (payload != null) {
            float x = PayloadHelper.decodeFloat(payload.bytes, payload.offset);
            return x;
        }
        return 1.0F;
    }

    @Override
    public SimScorer simScorer(SimWeight stats, AtomicReaderContext context) throws IOException {
      if (stats instanceof MultiSimilarity.MultiStats) {
        // a multi term query (e.g. phrase). return the summation, 
        // scoring almost as if it were boolean query
        SimWeight subStats[] = ((MultiSimilarity.MultiStats) stats).subStats;
        SimScorer subScorers[] = new SimScorer[subStats.length];
        for (int i = 0; i < subScorers.length; i++) {
          BasicStats basicstats = (BasicStats) subStats[i];
          subScorers[i] = new BasicSimScorer(basicstats, context.reader().getNormValues(basicstats.field));
        }
        return new MultiSimilarity.MultiSimScorer(subScorers);
      } else {
        BasicStats basicstats = (BasicStats) stats;
        return new BasicSimScorer(basicstats, context.reader().getNormValues(basicstats.field));
      }
    }
    
    private class BasicSimScorer extends SimScorer {
        private final BasicStats stats;
        private final NumericDocValues norms;
        
        BasicSimScorer(BasicStats stats, NumericDocValues norms) throws IOException {
          this.stats = stats;
          this.norms = norms;
        }
        
        @Override
        public float score(int doc, float freq) {
          // We have to supply something in case norms are omitted
          return ArticlePayloadIBSimilarity.this.score(stats, freq,
              norms == null ? 1F : decodeNormValue((byte)norms.get(doc)));
        }
        @Override
        public Explanation explain(int doc, Explanation freq) {
          return ArticlePayloadIBSimilarity.this.explain(stats, doc, freq,
              norms == null ? 1F : decodeNormValue((byte)norms.get(doc)));
        }

        @Override
        public float computeSlopFactor(int distance) {
          return 1.0f / (distance + 1);
        }

        @Override
        public float computePayloadFactor(int doc, int start, int end, BytesRef payload) {
          return scorePayload(doc, start, end, payload);
        }
      }
}
