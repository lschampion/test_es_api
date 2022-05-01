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
    //关闭连接
    @AfterEach
    public void closeClient(){
        try {
            esClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建索引
     */
    @Test
    public void testCreateIndex(){
        CreateIndexRequest request = new CreateIndexRequest(ES_INDEX);
        try {
            System.out.println(esClient);
            CreateIndexResponse response = esClient.indices().create(request, RequestOptions.DEFAULT);
            //响应状态
            boolean acknowledged = response.isAcknowledged();
            log.info("Index索引创建状态:{}",acknowledged);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询索引
     * @throws IOException
     */
    @Test
    public void searchIndex() throws IOException {
        // 查询索引
        GetIndexRequest request = new GetIndexRequest(ES_INDEX);
        GetIndexResponse response = esClient.indices().get(request, RequestOptions.DEFAULT);
        log.info("aliases {}",response.getAliases());
        log.info("mappings {}",response.getMappings());
        log.info("settings {}",response.getSettings());
    }

    /**
     * 删除索引
     * @throws IOException
     */
    @Test
    public void deleteIndex() throws IOException {
        //删除索引 - 请求对象
        DeleteIndexRequest request =new DeleteIndexRequest(ES_INDEX);
        //发送请求
        AcknowledgedResponse response = esClient.indices().delete(request, RequestOptions.DEFAULT);
        //操作结果
        log.info("操作结果:{}",response.isAcknowledged());
    }
    @Test
    public void testInsert() throws IOException {
        IndexRequest request = new IndexRequest();
        request.index("user").id("1001");

        User user = new User();
        user.setErpName("zhangsansan");
        user.setRealName("张三三");
        user.setPhone("12345654");
        user.setEmail("45456222333@qq.com");
        user.setPassword("13221");
        user.setIsDeleted(0);
        user.setCreateTime(new Date());
        user.setUpdateTime(new Date());
        //向ES插入数，必须转为JSON
        ObjectMapper mapper = new ObjectMapper();
        String userJson = mapper.writeValueAsString(user);
        request.source(userJson, XContentType.JSON);
        IndexResponse response = esClient.index(request, RequestOptions.DEFAULT);
        log.info("插入数据结果:{}", JSON.toJSONString(response));
    }

    /**
     *  修改文档
     * @throws IOException
     */
    @Test
    public void testUpdate() throws IOException {
        UpdateRequest request = new UpdateRequest();
        request.index("user").id("1001");
        request.doc(XContentType.JSON,"realName","李思思");
        UpdateResponse response = esClient.update(request, RequestOptions.DEFAULT);
        log.info("插入数据结果:{}", JSON.toJSONString(response));
    }

    /**
     * 查询文档
     * @throws IOException
     */
    @Test
    public void testSearch() throws IOException {
        GetRequest request = new GetRequest();
        request.index("user").id("1001");
        GetResponse response = esClient.get(request, RequestOptions.DEFAULT);
        String jsonString = response.getSourceAsString();
        log.info("查询结果:{}",jsonString);
    }

    /**
     * 删除文档s
     * @throws IOException
     */
    @Test
    public void testDelete() throws IOException {
        DeleteRequest request = new DeleteRequest();
        request.index("user").id("1001");
        DeleteResponse response = esClient.delete(request, RequestOptions.DEFAULT);
        log.info("删除结果:{}",JSON.toJSONString(response));
    }

    /**
     * 批量操作（新增）
     * @throws IOException
     */
    @Test
    public void testBatchInsert() throws IOException {
        BulkRequest request =new BulkRequest();
        request.add(new IndexRequest().index("user").id("1001").source(XContentType.JSON, "name", "zhangsan", "age", "10", "sex","女","birthday","2021-10-23"));
        request.add(new IndexRequest().index("user").id("1002").source(XContentType.JSON, "name", "lisi", "age", "30", "sex","女","birthday","2021-10-22"));
        request.add(new IndexRequest().index("user").id("1003").source(XContentType.JSON, "name", "wangwu1", "age", "40", "sex","男","birthday","2021-10-21"));
        request.add(new IndexRequest().index("user").id("1004").source(XContentType.JSON, "name", "wangwu2", "age", "20", "sex","女","birthday","2021-10-20"));
        request.add(new IndexRequest().index("user").id("1005").source(XContentType.JSON, "name", "wangwu3", "age", "50", "sex","男","birthday","2022-10-23"));
        request.add(new IndexRequest().index("user").id("1006").source(XContentType.JSON, "name", "wangwu4", "age", "20", "sex","男","birthday","2021-10-19"));
        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);
        log.info("批处理消耗的时间{},处理结果:{}",response.getTook(),response.getItems());
    }

    /**
     * 批量操作（删除）
     * @throws IOException
     */
    @Test
    public void testBatchDelete() throws IOException {
        BulkRequest request =new BulkRequest();
        request.add(new DeleteRequest().index("user").id("1001"));
        request.add(new DeleteRequest().index("user").id("1002"));
        request.add(new DeleteRequest().index("user").id("1003"));
        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);
        log.info("批处理消耗的时间 {}",response.getTook());
    }

    /**
     * 高级查询 前插入数据
     * @throws IOException
     */
    @Test
    public void testBatchInsert_() throws IOException {
        BulkRequest request =new BulkRequest();
        request.add(new IndexRequest().index("user").id("1001").source(XContentType.JSON, "name", "zhangsan", "age", 10, "sex","女","birthday","2021-10-23"));
        request.add(new IndexRequest().index("user").id("1002").source(XContentType.JSON, "name", "lisi", "age", 30, "sex","女","birthday","2021-10-22"));
        request.add(new IndexRequest().index("user").id("1003").source(XContentType.JSON, "name", "wangwu1", "age", 40, "sex","男","birthday","2021-10-21"));
        request.add(new IndexRequest().index("user").id("1004").source(XContentType.JSON, "name", "wangwu2", "age", 20, "sex","女","birthday","2021-10-20"));
        request.add(new IndexRequest().index("user").id("1005").source(XContentType.JSON, "name", "wangwu3", "age", 50, "sex","男","birthday","2022-10-23"));
        request.add(new IndexRequest().index("user").id("1006").source(XContentType.JSON, "name", "wangwu4", "age", 20, "sex","男","birthday","2021-10-19"));

        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);
        log.info("批处理消耗的时间{},处理结果:{}",response.getTook(),response.getItems());
    }

    /**
     * 查询所有索引数据
     * @throws IOException
     */
    @Test
    public void testSearchQuery() throws IOException {
        SearchRequest request =new SearchRequest();
        request.indices("user");
        //全量查询
        SearchSourceBuilder query = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        request.source(query);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();

        log.info("全量查询返回的结果数量:{}",hits.getTotalHits());
        //循环打印匹配到的数据
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 条件查询
     * term:查询条件为关键字
     * @throws IOException
     */
    @Test
    public void testSearchConditionQuery() throws IOException {
        SearchRequest request =new SearchRequest();
        request.indices("user");
        request.source(new SearchSourceBuilder().query(QueryBuilders.termQuery("age","30")));
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        log.info("全量查询返回的结果数量:{}",hits.getTotalHits());
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 条件查询
     * term:分页查询
     * @throws IOException
     */
    @Test
    public void testLimitPageQuery() throws IOException {
        SearchRequest request =new SearchRequest();
        request.indices("user");
        //全量查询
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        builder.from(0);
        builder.size(2);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        log.info("全量查询返回的结果数量:{}",hits.getTotalHits());
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }
    /**
     * 条件查询
     * term:排序
     * @throws IOException
     */
    @Test
    public void testOrder() throws IOException {
        SearchRequest request =new SearchRequest();
        request.indices("user");
        //全量查询
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        //排序
        builder.sort("age", SortOrder.DESC);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        log.info("全量查询返回的结果数量:{}",hits.getTotalHits());
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 过滤字段
     * @throws IOException
     */
    @Test
    public void testFilterFields() throws IOException {
        SearchRequest request =new SearchRequest();
        request.indices("user");
        //全量查询
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        String[] exclude = {"birthday"};
        String[] includes = {"name","age"};
        builder.fetchSource(includes,exclude);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        log.info("全量查询返回的结果数量:{}",hits.getTotalHits());
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * Bool查询
     * @throws IOException
     */
    @Test
    public void testCombinationQuery() throws IOException {
        SearchRequest request =new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // 必须包含
        boolQueryBuilder.must(QueryBuilders.termQuery("age", "30"));
        // 一定不含
//        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("name", "zhangsan"));
        // 可能包含
//        boolQueryBuilder.should(QueryBuilders.matchQuery("sex", "男"));
        builder.query(boolQueryBuilder);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        log.info("全量查询返回的结果数量:{}",hits.getTotalHits());
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 范围查找
     * @throws IOException
     */
    @Test
    public void testRangeQuery() throws IOException {
        SearchRequest request =new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("age");
        //大于等于
        rangeQuery.gte(20);
        //小于等于
        rangeQuery.lte(40);
        builder.query(rangeQuery);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        log.info("全量查询返回的结果数量:{}",hits.getTotalHits());
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 模糊搜索
     * @throws IOException
     */
    @Test
    public void testFuzzyQuery() throws IOException {
        SearchRequest request =new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();

        //Fuzziness.TWO 偏差wangwu的距离
        builder.query( QueryBuilders.fuzzyQuery("name","wangwu").fuzziness(Fuzziness.ONE));
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        log.info("查询返回的结果数量:{}",hits.getTotalHits());
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 高亮查询
     * @throws IOException
     */
    @Test
    public void testQuery() throws IOException {
        SearchRequest request =new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        //构建查询方式：高亮查询
        TermsQueryBuilder termsQueryBuilder =
                QueryBuilders.termsQuery("name","zhangsan");
        //设置查询方式
        builder.query(termsQueryBuilder);
        //构建高亮字段
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color='red'>");//设置标签前缀
        highlightBuilder.postTags("</font>");//设置标签后缀
        highlightBuilder.field("name");//设置高亮字段

        //设置高亮构建对象
        builder.highlighter(highlightBuilder);
        //设置请求体
        request.source(builder);
        //3.客户端发送请求，获取响应对象
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        //4.打印响应结果
        SearchHits hits = response.getHits();
        System.out.println("took::"+response.getTook());
        System.out.println("time_out::"+response.isTimedOut());
        System.out.println("total::"+hits.getTotalHits());
        System.out.println("max_score::"+hits.getMaxScore());
        System.out.println("hits::::>>");
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
            //打印高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            System.out.println(highlightFields);
        }
        System.out.println("<<::::");
    }

    /**
     * 聚合查询:max --> "Fielddata is disabled on text fields by default.
     * @throws IOException
     */
    @Test
    public void testAggregateQuery() throws IOException {
        SearchRequest request = new SearchRequest().indices("user");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(AggregationBuilders.max("maxAge").field("age"));
        //设置请求体
        request.source(sourceBuilder);
        //3.客户端发送请求，获取响应对象
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        //4.打印响应结果
        SearchHits hits = response.getHits();
        log.info("查询返回的结果数量:{}",hits.getTotalHits());
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 条件分组
     * @throws IOException
     */
    @Test
    public void testGroupSearch() throws IOException {
        SearchRequest request = new SearchRequest("user");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(AggregationBuilders.terms("age_groupby").field("age"));
        //设置请求体
        request.source(sourceBuilder);
        //3.客户端发送请求，获取响应对象
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        log.info("查询返回的结果{}", JSON.toJSONString(response));
    }



}
