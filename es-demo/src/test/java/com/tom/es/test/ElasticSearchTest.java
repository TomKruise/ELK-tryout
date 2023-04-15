package com.tom.es.test;

import com.google.gson.Gson;
import com.tom.es.pojo.Item;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

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
}