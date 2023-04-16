package com.tom.es.test;

import com.google.gson.Gson;
import com.tom.es.pojo.Item;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticSearchTest {
    private RestHighLevelClient client;
    private Gson gson = new Gson();

    @Before
    public void init() {
        client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://127.0.0.1:9200")
        ));
    }

    @After
    public void close() throws IOException {
        client.close();
    }

    @Test
    public void testCreateIndex() throws IOException {
        Item item = new Item(1L,"iphone X","手机","Apple",8900.00,"http://localhost");

        String jsonStr = gson.toJson(item);

        IndexRequest indexRequest = new IndexRequest("item", "docs", item.getId().toString()).source(jsonStr, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

        System.out.println("response: " + indexResponse);
    }

    @Test
    public void testGetIndex() throws IOException {
        GetRequest getRequest = new GetRequest("item", "docs", "1");

        //结果source过滤
//        getRequest.fetchSourceContext(new FetchSourceContext(true, null, null));
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);

        String sourceAsString = getResponse.getSourceAsString();

        Item item = gson.fromJson(sourceAsString, Item.class);

        System.out.println(item);
    }

    @Test
    public void testDeleteIndex() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("item", "docs", "1");

        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);

        System.out.println("response: " + deleteResponse);
    }

    @Test
    public void testBulkIndex() throws IOException {
        List<Item> list = new ArrayList<>();
        list.add(new Item(1L,"iphone 8","手机","Apple",8900.00,"http://localhost/"));
        list.add(new Item(2L,"iphone X","手机","Apple",9900.00,"http://localhost/"));
        list.add(new Item(3L,"iphone XR","手机","Apple",10900.00,"http://localhost/"));
        list.add(new Item(4L,"iphone XS","手机","Apple",11900.00,"http://localhost/"));
        list.add(new Item(5L,"iphone 11","手机","Apple",12900.00,"http://localhost/"));
        list.add(new Item(6L,"iphone 12","手机","Apple",13900.00,"http://localhost/"));
        list.add(new Item(7L,"iphone 13","手机","Apple",14900.00,"http://localhost/"));
        list.add(new Item(8L,"iphone 14","手机","Apple",15900.00,"http://localhost/"));

        BulkRequest bulkRequest = new BulkRequest();
        for (Item item : list) {
            bulkRequest.add(new IndexRequest("item", "docs", item.getId().toString()).source(gson.toJson(item), XContentType.JSON));
        }
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println("response: " + bulkResponse);
    }

    @Test
    public void testMatchAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.query(QueryBuilders.matchAllQuery());

        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits hits = searchResponse.getHits();
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit documentFields : hits1) {
            String json = documentFields.getSourceAsString();
            Item item = gson.fromJson(json, Item.class);
            System.out.println("item: "+item);
        }
    }

    @Test
    public void testMatch() throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.query(QueryBuilders.matchQuery("title","iphone"));

        basicQueryTemplate(sourceBuilder);
    }

    @Test
    public void testRange() throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.query(QueryBuilders.rangeQuery("price").gt(2000).lt(9000));

        basicQueryTemplate(sourceBuilder);
    }

    @Test
    public void testSourceFilter() throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.query(QueryBuilders.matchAllQuery());

        sourceBuilder.fetchSource(new String[]{"id","title","price"},null);

        basicQueryTemplate(sourceBuilder);
    }

    @Test
    public void testSortQuery() throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.query(QueryBuilders.matchAllQuery());

        sourceBuilder.sort("price", SortOrder.ASC);

        basicQueryTemplate(sourceBuilder);
    }

    @Test
    public void testSortAndPageQuery() throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.query(QueryBuilders.matchAllQuery());

        sourceBuilder.sort("price", SortOrder.ASC);

        int page=1;
        int size=3;
        int start=(page-1)*size;

        sourceBuilder.from(start);
        sourceBuilder.size(size);

        basicQueryTemplate(sourceBuilder);
    }

    @Test
    public void testAggregation() throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.query(QueryBuilders.matchAllQuery());

        sourceBuilder.sort("price", SortOrder.ASC);

        sourceBuilder.size();

        sourceBuilder.aggregation(AggregationBuilders.terms("brandAgg").field("brand"));

        SearchRequest searchRequest = new SearchRequest();

        searchRequest.indices("item");

        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        Aggregations aggregations = searchResponse.getAggregations();

        Terms terms = aggregations.get("brandAgg");

        for (Terms.Bucket bucket : terms.getBuckets()) {
            System.out.println("brand: " + bucket.getKeyAsString() + " count: " + bucket.getDocCount());
        }
    }

    private void basicQueryTemplate(SearchSourceBuilder sourceBuilder) throws IOException {
        SearchRequest searchRequest = new SearchRequest();

        searchRequest.indices("item");

        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits hits = searchResponse.getHits();
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit hit : hits1) {
            String json = hit.getSourceAsString();
            Item item = gson.fromJson(json, Item.class);
            System.out.println("item: "+item);
        }
    }
}