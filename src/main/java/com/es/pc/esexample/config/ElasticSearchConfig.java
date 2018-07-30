package com.es.pc.esexample.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.context.annotation.Configuration;

import java.util.logging.Level;
import java.util.logging.Logger;

@Configuration
public class ElasticSearchConfig extends AbstractFactoryBean {

    private static final Logger logger = Logger.getLogger(ElasticSearchConfig.class.getName());

    @Value("${ELASTICSEARCH_URL:https://search-pc-example-yiio4l2uvri3fuerh6i762a27q.us-west-2.es.amazonaws.com}")
    private String elasticsearchURL;

    private RestHighLevelClient restHighLevelClient;

    @Override
    public void destroy() {
        try {
            if (restHighLevelClient != null) {
                restHighLevelClient.close();
            }
        } catch (final Exception e) {
            logger.log(Level.SEVERE, "Error closing ElasticSearch client: ", e);
        }
    }

    @Override
    public Class<RestHighLevelClient> getObjectType() {
        return RestHighLevelClient.class;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    @Override
    public RestHighLevelClient createInstance() {
        return buildClient();
    }

    private RestHighLevelClient buildClient() {
        try {
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(HttpHost.create(elasticsearchURL)));

        } catch (Exception e) {
            logger.log(Level.SEVERE, "FAILED creation of the Elasticsearch configuration: ", e);
        }
        return restHighLevelClient;
    }
}
