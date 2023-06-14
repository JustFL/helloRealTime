package com.javbus.publisher.mapper;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
@Slf4j
public class DauMapperImpl implements DauMapper {

    @Autowired
    RestHighLevelClient restHighLevelClient;

    private final String dauIndexNamePrefix = "mall_dau_";

    @Override public HashMap<String, Object> gainDau(String dt) {
        val indexName = dauIndexNamePrefix + dt;
        HashMap<String, Object> map = new HashMap<>();

        Long dauTotal = searchDauTotal(indexName);
        map.put("dauTotal", dauTotal);

        Map<String, Long> dauTd = searchDauHr(indexName);
        map.put("dauTd", dauTd);

        LocalDate now = LocalDate.now();
        LocalDate yesterday = now.minusDays(1);
        Map<String, Long> dauYd = searchDauHr(dauIndexNamePrefix + yesterday);
        map.put("dauYd", dauYd);

        return map;
    }

    public Long searchDauTotal(String indexName) {
        // ctrl+alt+t
        try {
            // SearchSourceBuilder
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(0);
            // SearchRequest
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            // 获取结果
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            return response.getHits().getTotalHits().value;
        } catch (ElasticsearchStatusException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + " is not exist...");
            }
        } catch (IOException e) {
            throw new RuntimeException("search fail...");
        }
        return 0L;
    }


    public Map<String, Long> searchDauHr(String indexName) {
        HashMap<String, Long> map = new HashMap<String, Long>();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 不要明细
        searchSourceBuilder.size(0);
        // 聚合
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("groupByHr").field("hr.keyword").size(30);
        searchSourceBuilder.aggregation(aggregationBuilder);
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = response.getAggregations();
            ParsedTerms parsedTerms = aggregations.get("groupByHr");
            List<? extends Terms.Bucket> buckets = parsedTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                map.put(bucket.getKeyAsString(), bucket.getDocCount());
            }
            return map;
        } catch (ElasticsearchStatusException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + " is not exist...");
            }
        } catch (IOException e) {
            throw new RuntimeException("search fail...");
        }
        return null;
    }

}
