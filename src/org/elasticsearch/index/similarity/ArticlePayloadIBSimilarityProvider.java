package org.elasticsearch.index.similarity;

import org.apache.lucene.search.similarities.ArticlePayloadIBSimilarity;
import org.apache.lucene.search.similarities.Distribution;
import org.apache.lucene.search.similarities.DistributionLL;
import org.apache.lucene.search.similarities.DistributionSPL;
import org.apache.lucene.search.similarities.Lambda;
import org.apache.lucene.search.similarities.LambdaDF;
import org.apache.lucene.search.similarities.LambdaTTF;
import org.apache.lucene.search.similarities.Normalization;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;

public class ArticlePayloadIBSimilarityProvider extends AbstractSimilarityProvider {

    private static final ImmutableMap<String, Distribution> DISTRIBUTION_CACHE;
    private static final ImmutableMap<String, Lambda> LAMBDA_CACHE;

    static {
        MapBuilder<String, Distribution> distributions = MapBuilder.newMapBuilder();
        distributions.put("ll", new DistributionLL());
        distributions.put("spl", new DistributionSPL());
        DISTRIBUTION_CACHE = distributions.immutableMap();

        MapBuilder<String, Lambda> lamdas = MapBuilder.newMapBuilder();
        lamdas.put("df", new LambdaDF());
        lamdas.put("ttf", new LambdaTTF());
        LAMBDA_CACHE = lamdas.immutableMap();
    }
    private ArticlePayloadIBSimilarity similarity;
    
    //public ArticlePayloadIBSimilarityProvider(String name, Settings settings) {
    //    super(name, settings);
    //}

    @Inject
    public ArticlePayloadIBSimilarityProvider(@Assisted String name, @Assisted Settings settings) {
        super(name);
        Distribution distribution = parseDistribution(settings);
        Lambda lambda = parseLambda(settings);
        Normalization normalization = parseNormalization(settings);
        this.similarity = new ArticlePayloadIBSimilarity(distribution, lambda, normalization);
    }

    /**
     * Parses the given Settings and creates the appropriate {@link Distribution}
     *
     * @param settings Settings to parse
     * @return {@link Normalization} referred to in the Settings
     */
    protected Distribution parseDistribution(Settings settings) {
        String rawDistribution = settings.get("distribution");
        Distribution distribution = DISTRIBUTION_CACHE.get(rawDistribution);
        if (distribution == null) {
            throw new ElasticsearchIllegalArgumentException("Unsupported Distribution [" + rawDistribution + "]");
        }
        return distribution;
    }

    /**
     * Parses the given Settings and creates the appropriate {@link Lambda}
     *
     * @param settings Settings to parse
     * @return {@link Normalization} referred to in the Settings
     */
    protected Lambda parseLambda(Settings settings) {
        String rawLambda = settings.get("lambda");
        Lambda lambda = LAMBDA_CACHE.get(rawLambda);
        if (lambda == null) {
            throw new ElasticsearchIllegalArgumentException("Unsupported Lambda [" + rawLambda + "]");
        }
        return lambda;
    }
    
    public ArticlePayloadIBSimilarity get() {
        return similarity;
    }
}
