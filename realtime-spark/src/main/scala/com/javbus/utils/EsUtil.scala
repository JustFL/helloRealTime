package com.javbus.utils

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.{GetRequest, GetResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, Requests, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType


object EsUtil {

  def getClient: RestHighLevelClient = {
    val builder: RestClientBuilder = RestClient.builder(new HttpHost("bigdata01", 9200), new HttpHost("bigdata02", 9200))
    val client: RestHighLevelClient = new RestHighLevelClient(builder)
    client
  }

  // 幂等写入 (指定id)
  def save(indexName: String, id: String, jsonData: String): Unit = {
    val client: RestHighLevelClient = getClient
    val request: IndexRequest = Requests.indexRequest(indexName)
    request.id(id)
    request.source(jsonData, XContentType.JSON)
    client.index(request, RequestOptions.DEFAULT)
    client.close()
  }

  def update(indexName: String, docId: String, key: String, value: String) = {
    val client: RestHighLevelClient = getClient
    val updateRequest: UpdateRequest = new UpdateRequest(indexName, docId)
    updateRequest.doc(key, value)
    client.update(updateRequest, RequestOptions.DEFAULT)
    client.close()
  }

  // 幂等批量写入
  def batch(indexName: String, datas: List[(String, AnyRef)]): Unit = {
    val client: RestHighLevelClient = getClient
    val bulkRequest: BulkRequest = Requests.bulkRequest()
    for ((docId, obj) <- datas) {
      val request: IndexRequest = new IndexRequest(indexName)
      request.id(docId)
      request.source(JSON.toJSONString(obj, new SerializeConfig(true)), XContentType.JSON)
      bulkRequest.add(request)
    }
    client.bulk(bulkRequest, RequestOptions.DEFAULT)
    client.close()
  }

  def delete(indexName: String, id: String) = {
    val client: RestHighLevelClient = getClient
    val deleterequest: DeleteRequest = Requests.deleteRequest(indexName)
    deleterequest.id(id)
    client.delete(deleterequest, RequestOptions.DEFAULT)
    client.close()
  }

  def getById(indexName: String, id: String): String = {
    val client: RestHighLevelClient = getClient
    val getRequest: GetRequest = new GetRequest(indexName, id)
    val response: GetResponse = client.get(getRequest, RequestOptions.DEFAULT)
    val result: String = response.getSourceAsString
    client.close()
    result
  }
}

