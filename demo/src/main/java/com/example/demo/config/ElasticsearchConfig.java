package com.example.demo.config;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.params.HttpParams;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.io.IOException;

@Configuration
public class ElasticsearchConfig implements InitializingBean, DisposableBean {

    private final static Logger logger = LoggerFactory.getLogger(ElasticsearchConfig.class);


    @Resource
    private RestHighLevelClient restHighLevelClient;

    /**
     * ben 销毁时 关闭ES连接
     *
     * @throws Exception
     */
    @Override
    public void destroy() throws Exception {
        // 关闭Elasticsearch连接
        logger.info("...........................................关闭Elasticsearch连接.......");

        try {
            this.restHighLevelClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * bean 创建时 初始化 ES连接
     *
     * @throws Exception
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("............................................初始化Elasticsearch连接.......");
        restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));

    }


    /**
     * 批量创建文档
     * @param bulkRequest
     * @return
     * @throws IOException
     */
    public BulkResponse  bulk(BulkRequest bulkRequest) throws IOException{
        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        return bulk;

    }

    /**
     * scroll 滚动查询
     * @param scrollRequest
     * @return
     * @throws IOException
     */
    public SearchResponse  scroll(SearchScrollRequest  scrollRequest)throws  IOException{
        SearchResponse scroll = restHighLevelClient.scroll(scrollRequest, RequestOptions.DEFAULT);
        return scroll;

    }


    /**
     * 关闭Scroll链接
     */
    public void clearScroll(String scrollId) throws IOException {
        // 清除 Scroll 连接
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);

        restHighLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);

    }

    /**
     * 保存
     *
     * @param indexRequest
     * @throws IOException
     */

    public IndexResponse save(IndexRequest indexRequest) throws IOException {
        IndexResponse index = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        return index;
    }

    /**
     * 查询
     *
     * @param request
     * @throws IOException
     */
    public SearchResponse query(SearchRequest request) throws IOException {
        SearchResponse search = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        return search;
    }


    /**
     * 判断是否存在
     *
     * @param getRequest
     * @return
     * @throws IOException
     */
    public Boolean exists(GetRequest getRequest) throws IOException {
        boolean exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
        return exists;
    }


    /**
     * 修改
     *
     * @param updateRequest
     * @return
     * @throws IOException
     */
    public UpdateResponse update(UpdateRequest updateRequest) throws IOException {
        UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        return update;
    }


    /**
     * 修改
     *
     * @param updateRequest
     * @return
     * @throws IOException
     */
    public BulkByScrollResponse updateQuery(UpdateByQueryRequest updateRequest) throws IOException {
        BulkByScrollResponse update = restHighLevelClient.updateByQuery(updateRequest, RequestOptions.DEFAULT);
        return update;
    }

    /**
     * 删除
     *
     * @param deleteRequest
     * @return
     * @throws IOException
     */
    public DeleteResponse dal(DeleteRequest deleteRequest) throws IOException {
        DeleteResponse delete = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        return delete;
    }

}
