package com.lisacumt.test_es.API_test;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

@Slf4j
public class ESTest {
    public static RestHighLevelClient esClient;
    public final static String ES_INDEX="foodie_shop";
    @BeforeEach
    public void getEsClient(){
        ESRestClient esRestClient=new ESRestClient();
        esClient= esRestClient.initRestClient();
    }
    //????????????
    @AfterEach
    public void closeClient(){
        try {
            esClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * ????????????
     */
    @Test
    public void testCreateIndex(){
        CreateIndexRequest request = new CreateIndexRequest(ES_INDEX);
        try {
            System.out.println(esClient);
            CreateIndexResponse response = esClient.indices().create(request, RequestOptions.DEFAULT);
            //????????????
            boolean acknowledged = response.isAcknowledged();
            log.info("Index??????????????????:{}",acknowledged);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * ????????????
     * @throws IOException
     */
    @Test
    public void searchIndex() throws IOException {
        // ????????????
        GetIndexRequest request = new GetIndexRequest(ES_INDEX);
        GetIndexResponse response = esClient.indices().get(request, RequestOptions.DEFAULT);
        log.info("aliases {}",response.getAliases());
        log.info("mappings {}",response.getMappings());
        log.info("settings {}",response.getSettings());
    }

    /**
     * ????????????
     * @throws IOException
     */
    @Test
    public void deleteIndex() throws IOException {
        //???????????? - ????????????
        DeleteIndexRequest request =new DeleteIndexRequest(ES_INDEX);
        //????????????
        AcknowledgedResponse response = esClient.indices().delete(request, RequestOptions.DEFAULT);
        //????????????
        log.info("????????????:{}",response.isAcknowledged());
    }
    @Test
    public void testInsert() throws IOException {
        IndexRequest request = new IndexRequest();
        request.index("user").id("1001");

        User user = new User();
        user.setErpName("zhangsansan");
        user.setRealName("?????????");
        user.setPhone("12345654");
        user.setEmail("45456222333@qq.com");
        user.setPassword("13221");
        user.setIsDeleted(0);
        user.setCreateTime(new Date());
        user.setUpdateTime(new Date());
        //???ES????????????????????????JSON
        ObjectMapper mapper = new ObjectMapper();
        String userJson = mapper.writeValueAsString(user);
        request.source(userJson, XContentType.JSON);
        IndexResponse response = esClient.index(request, RequestOptions.DEFAULT);
        log.info("??????????????????:{}", JSON.toJSONString(response));
    }

    /**
     *  ????????????
     * @throws IOException
     */
    @Test
    public void testUpdate() throws IOException {
        UpdateRequest request = new UpdateRequest();
        request.index("user").id("1001");
        request.doc(XContentType.JSON,"realName","?????????");
        UpdateResponse response = esClient.update(request, RequestOptions.DEFAULT);
        log.info("??????????????????:{}", JSON.toJSONString(response));
    }

    /**
     * ????????????
     * @throws IOException
     */
    @Test
    public void testSearch() throws IOException {
        GetRequest request = new GetRequest();
        request.index("user").id("1001");
        GetResponse response = esClient.get(request, RequestOptions.DEFAULT);
        String jsonString = response.getSourceAsString();
        log.info("????????????:{}",jsonString);
    }

    /**
     * ????????????s
     * @throws IOException
     */
    @Test
    public void testDelete() throws IOException {
        DeleteRequest request = new DeleteRequest();
        request.index("user").id("1001");
        DeleteResponse response = esClient.delete(request, RequestOptions.DEFAULT);
        log.info("????????????:{}",JSON.toJSONString(response));
    }

    /**
     * ????????????????????????
     * @throws IOException
     */
    @Test
    public void testBatchInsert() throws IOException {
        BulkRequest request =new BulkRequest();
        request.add(new IndexRequest().index("user").id("1001").source(XContentType.JSON, "name", "zhangsan", "age", "10", "sex","???","birthday","2021-10-23"));
        request.add(new IndexRequest().index("user").id("1002").source(XContentType.JSON, "name", "lisi", "age", "30", "sex","???","birthday","2021-10-22"));
        request.add(new IndexRequest().index("user").id("1003").source(XContentType.JSON, "name", "wangwu1", "age", "40", "sex","???","birthday","2021-10-21"));
        request.add(new IndexRequest().index("user").id("1004").source(XContentType.JSON, "name", "wangwu2", "age", "20", "sex","???","birthday","2021-10-20"));
        request.add(new IndexRequest().index("user").id("1005").source(XContentType.JSON, "name", "wangwu3", "age", "50", "sex","???","birthday","2022-10-23"));
        request.add(new IndexRequest().index("user").id("1006").source(XContentType.JSON, "name", "wangwu4", "age", "20", "sex","???","birthday","2021-10-19"));
        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);
        log.info("????????????????????????{},????????????:{}",response.getTook(),response.getItems());
    }

    /**
     * ????????????????????????
     * @throws IOException
     */
    @Test
    public void testBatchDelete() throws IOException {
        BulkRequest request =new BulkRequest();
        request.add(new DeleteRequest().index("user").id("1001"));
        request.add(new DeleteRequest().index("user").id("1002"));
        request.add(new DeleteRequest().index("user").id("1003"));
        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);
        log.info("???????????????????????? {}",response.getTook());
    }

    /**
     * ???????????? ???????????????
     * @throws IOException
     */
    @Test
    public void testBatchInsert_() throws IOException {
        BulkRequest request =new BulkRequest();
        request.add(new IndexRequest().index("user").id("1001").source(XContentType.JSON, "name", "zhangsan", "age", 10, "sex","???","birthday","2021-10-23"));
        request.add(new IndexRequest().index("user").id("1002").source(XContentType.JSON, "name", "lisi", "age", 30, "sex","???","birthday","2021-10-22"));
        request.add(new IndexRequest().index("user").id("1003").source(XContentType.JSON, "name", "wangwu1", "age", 40, "sex","???","birthday","2021-10-21"));
        request.add(new IndexRequest().index("user").id("1004").source(XContentType.JSON, "name", "wangwu2", "age", 20, "sex","???","birthday","2021-10-20"));
        request.add(new IndexRequest().index("user").id("1005").source(XContentType.JSON, "name", "wangwu3", "age", 50, "sex","???","birthday","2022-10-23"));
        request.add(new IndexRequest().index("user").id("1006").source(XContentType.JSON, "name", "wangwu4", "age", 20, "sex","???","birthday","2021-10-19"));

        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);
        log.info("????????????????????????{},????????????:{}",response.getTook(),response.getItems());
    }

    /**
     * ????????????????????????
     * @throws IOException
     */
    @Test
    public void testSearchQuery() throws IOException {
        SearchRequest request =new SearchRequest();
        request.indices("user");
        //????????????
        SearchSourceBuilder query = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        request.source(query);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();

        log.info("?????????????????????????????????:{}",hits.getTotalHits());
        //??????????????????????????????
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * ????????????
     * term:????????????????????????
     * @throws IOException
     */
    @Test
    public void testSearchConditionQuery() throws IOException {
        SearchRequest request =new SearchRequest();
        request.indices("user");
        request.source(new SearchSourceBuilder().query(QueryBuilders.termQuery("age","30")));
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        log.info("?????????????????????????????????:{}",hits.getTotalHits());
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * ????????????
     * term:????????????
     * @throws IOException
     */
    @Test
    public void testLimitPageQuery() throws IOException {
        SearchRequest request =new SearchRequest();
        request.indices("user");
        //????????????
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        builder.from(0);
        builder.size(2);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        log.info("?????????????????????????????????:{}",hits.getTotalHits());
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }
    /**
     * ????????????
     * term:??????
     * @throws IOException
     */
    @Test
    public void testOrder() throws IOException {
        SearchRequest request =new SearchRequest();
        request.indices("user");
        //????????????
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        //??????
        builder.sort("age", SortOrder.DESC);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        log.info("?????????????????????????????????:{}",hits.getTotalHits());
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * ????????????
     * @throws IOException
     */
    @Test
    public void testFilterFields() throws IOException {
        SearchRequest request =new SearchRequest();
        request.indices("user");
        //????????????
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        String[] exclude = {"birthday"};
        String[] includes = {"name","age"};
        builder.fetchSource(includes,exclude);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        log.info("?????????????????????????????????:{}",hits.getTotalHits());
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * Bool??????
     * @throws IOException
     */
    @Test
    public void testCombinationQuery() throws IOException {
        SearchRequest request =new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // ????????????
        boolQueryBuilder.must(QueryBuilders.termQuery("age", "30"));
        // ????????????
//        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("name", "zhangsan"));
        // ????????????
//        boolQueryBuilder.should(QueryBuilders.matchQuery("sex", "???"));
        builder.query(boolQueryBuilder);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        log.info("?????????????????????????????????:{}",hits.getTotalHits());
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * ????????????
     * @throws IOException
     */
    @Test
    public void testRangeQuery() throws IOException {
        SearchRequest request =new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("age");
        //????????????
        rangeQuery.gte(20);
        //????????????
        rangeQuery.lte(40);
        builder.query(rangeQuery);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        log.info("?????????????????????????????????:{}",hits.getTotalHits());
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * ????????????
     * @throws IOException
     */
    @Test
    public void testFuzzyQuery() throws IOException {
        SearchRequest request =new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();

        //Fuzziness.TWO ??????wangwu?????????
        builder.query( QueryBuilders.fuzzyQuery("name","wangwu").fuzziness(Fuzziness.ONE));
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        log.info("???????????????????????????:{}",hits.getTotalHits());
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * ????????????
     * @throws IOException
     */
    @Test
    public void testQuery() throws IOException {
        SearchRequest request =new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        //?????????????????????????????????
        TermsQueryBuilder termsQueryBuilder =
                QueryBuilders.termsQuery("name","zhangsan");
        //??????????????????
        builder.query(termsQueryBuilder);
        //??????????????????
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color='red'>");//??????????????????
        highlightBuilder.postTags("</font>");//??????????????????
        highlightBuilder.field("name");//??????????????????

        //????????????????????????
        builder.highlighter(highlightBuilder);
        //???????????????
        request.source(builder);
        //3.??????????????????????????????????????????
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        //4.??????????????????
        SearchHits hits = response.getHits();
        System.out.println("took::"+response.getTook());
        System.out.println("time_out::"+response.isTimedOut());
        System.out.println("total::"+hits.getTotalHits());
        System.out.println("max_score::"+hits.getMaxScore());
        System.out.println("hits::::>>");
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
            //??????????????????
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            System.out.println(highlightFields);
        }
        System.out.println("<<::::");
    }

    /**
     * ????????????:max --> "Fielddata is disabled on text fields by default.
     * @throws IOException
     */
    @Test
    public void testAggregateQuery() throws IOException {
        SearchRequest request = new SearchRequest().indices("user");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(AggregationBuilders.max("maxAge").field("age"));
        //???????????????
        request.source(sourceBuilder);
        //3.??????????????????????????????????????????
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        //4.??????????????????
        SearchHits hits = response.getHits();
        log.info("???????????????????????????:{}",hits.getTotalHits());
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * ????????????
     * @throws IOException
     */
    @Test
    public void testGroupSearch() throws IOException {
        SearchRequest request = new SearchRequest("user");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(AggregationBuilders.terms("age_groupby").field("age"));
        //???????????????
        request.source(sourceBuilder);
        //3.??????????????????????????????????????????
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        log.info("?????????????????????{}", JSON.toJSONString(response));
    }



}
