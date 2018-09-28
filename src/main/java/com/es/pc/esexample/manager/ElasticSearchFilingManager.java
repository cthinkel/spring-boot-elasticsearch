package com.es.pc.esexample.manager;

import com.es.pc.esexample.service.FilingService;
import com.es.pc.esexample.utils.FilingUtils;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

@Component
public class ElasticSearchFilingManager {

    private static Logger logger = Logger.getLogger(FilingService.class.getName());

    @Autowired
    private RestHighLevelClient restHighLevelClient;


    private static final String FILING_INDEX = "filings";
    private static final String FILING_TYPE = "filing";

    private static final String FIELD_PLAN_NAME = "PLAN_NAME";
    private static final String FIELD_FILING_SPONSOR_NAME = "SPONSOR_DFE_NAME";
    private static final String FIELD_FILING_SPONSOR_STATE = "SPONS_DFE_LOC_US_STATE";

    public List<Map<String, Object>> findFilings(String searchQuery, String fieldName) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(FILING_INDEX);
        searchRequest.types(FILING_TYPE);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        QueryBuilder query = QueryBuilders.queryStringQuery(searchQuery)
                .defaultField(fieldName)
                .defaultOperator(Operator.AND);

        sourceBuilder.query(query);
        searchRequest.source(sourceBuilder);
        logger.log(Level.INFO, "Search Request: " + searchRequest);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
        SearchHits searchHits = searchResponse.getHits();
        int matchDocuments = (int) searchHits.totalHits;

        logger.log(Level.INFO, "Found: " + matchDocuments + " matches");

        return extractFilingsFromResponse(searchResponse);
    }

    public List<Map<String, Object>> findFilingsByPlanName(String planName) throws IOException {
        return findFilings(planName, FIELD_PLAN_NAME);
    }

    public List<Map<String, Object>> findFilingsBySponsorName(String sponsorName) throws IOException {
        return findFilings(sponsorName, FIELD_FILING_SPONSOR_NAME);
    }

    public List<Map<String, Object>> findFilingsBySponsorState(String sponsorState) throws IOException {
        return findFilings(sponsorState, FIELD_FILING_SPONSOR_STATE);
    }

    private List<Map<String, Object>> extractFilingsFromResponse(SearchResponse searchResponse) throws IOException {
        List<Map<String, Object>> filings = new ArrayList<>();
        for (SearchHit searchHit : searchResponse.getHits()) {
            Map<String, Object> filing = FilingUtils.fromJSON(searchHit.getSourceAsString());
            filings.add(filing);
        }
        return filings;
    }

    public void processCsv(List<Map<String, String>> records) throws IOException {
        BulkRequest request = new BulkRequest();

        for (Map<String, String> record : records) {
            IndexRequest indexRequest = new IndexRequest(FILING_INDEX, FILING_TYPE).source(FilingUtils.toJSON(record), XContentType.JSON);
            request.add(indexRequest);
        }

        request.timeout(TimeValue.timeValueMinutes(2));
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        BulkResponse bulkResponse = restHighLevelClient.bulk(request);
        if (bulkResponse.hasFailures()) {
            System.out.println("bad news bears: " + bulkResponse.buildFailureMessage());
        }
    }
}
