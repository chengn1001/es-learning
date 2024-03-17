package com.example.demo.controller;

import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.ElasticsearchConfig;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
public class TestController {

    private static final Logger logger = LoggerFactory.getLogger(TestController.class);


    @Autowired
    private ElasticsearchConfig elasticsearchConfig;


    @RequestMapping("test")
    public String test(){
        logger.info("testttttttttsssssstttttttt");
        return "test";
    }



    @RequestMapping("testEs")
    public JSONObject query() {
        try {
            SearchRequest request = new SearchRequest("student_info");
            SearchSourceBuilder builder = new SearchSourceBuilder();

            //多个条件 构建SearchSourceBuilder 查询参数 构建一个精确匹配查询，即查找指定字段（field）等于指定值（value）的文档。
            //builder.query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("user","张三12")).must(QueryBuilders.termQuery("name","java 实现23更新")));

            //rangeQuery(String field): 构建一个范围查询，可以指定字段的值在一个范围内   gt 大于 gte 大于等于 lt 小于 lte 小于等于
            // builder.query(QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("age").gt("40").lt("50")));

            //前缀模糊匹配  对应mysql 中的 %
            //  builder.query(QueryBuilders.matchPhrasePrefixQuery("user","张858"));

            //多个值查询  对应mysql 中的in
            //   builder.query(QueryBuilders.termsQuery("age", queryInt));
            //默认从0开始
            builder.from(0);
            //分页的大小，默认为10，当查询数量超过1万的时候 会进行深分页
            builder.size(23);

            request.source(builder);
            SearchResponse query = elasticsearchConfig.query(request);
            List<Map<String, Object>> list = new ArrayList<>();

            for (SearchHit hit : query.getHits().getHits()) {
                logger.info("查询ES 结果{}", JSONObject.toJSONString(hit.getSourceAsMap()));
                list.add(hit.getSourceAsMap());
            }
            JSONObject result = new JSONObject();
            result.put("data", list);
            return result;

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

    }


}
