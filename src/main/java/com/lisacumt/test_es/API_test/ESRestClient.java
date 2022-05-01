package com.lisacumt.test_es.API_test;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

public class ESRestClient {
    public RestHighLevelClient initRestClient() {

        RestClientBuilder builder= null;
        builder = RestClient.builder(new HttpHost("192.168.100.110", 9200));
        return new RestHighLevelClient(builder);
    }
}
