package com.sibat;


import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ElasticSearchOperator {

    // 缓冲池容量
    private static final int MAX_BULK_COUNT = 1000;
    // 最大提交间隔（秒）
    private static final int MAX_COMMIT_INTERVAL = 60 * 5;

    private static TransportClient client = null;
    private static BulkRequestBuilder bulkRequestBuilder = null;

    private static Lock commitLock = new ReentrantLock();

    static {
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", Config.clusterName).build();
        try {
            client = TransportClient.builder().settings(settings).build();
            for (String ip : Config.nodeHost.split(",")) {
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip), 9300));
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.setRefresh(true);

        Timer timer = new Timer();
        timer.schedule(new CommitTimer(), 10 * 1000, MAX_COMMIT_INTERVAL * 1000);
    }

    /**
     * 判断缓存池是否已满，批量提交
     *
     * @param threshold
     */
    private static void bulkRequest(int threshold) {
        if (bulkRequestBuilder.numberOfActions() > threshold) {
            BulkResponse bulkResponse = bulkRequestBuilder.get();
            if (!bulkResponse.hasFailures()) {
                bulkRequestBuilder = client.prepareBulk();
            }
        }
    }

    /**
     * 加入索引请求到缓冲池
     *
     * @param builder
     */
    public static void addUpdateBuilderToBulk(IndexRequestBuilder builder) {
        commitLock.lock();
        try {
            bulkRequestBuilder.add(builder);
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            commitLock.unlock();
        }
    }

    /**
     * 加入删除请求到缓冲池
     *
     * @param builder
     */
    public static void addDeleteBuilderToBulk(DeleteRequestBuilder builder) {
        commitLock.lock();
        try {
            bulkRequestBuilder.add(builder);
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            commitLock.unlock();
        }
    }

    /**
     * 定时任务，避免RegionServer迟迟无数据更新，导致ElasticSearch没有与HBase同步
     */
    static class CommitTimer extends TimerTask {
        @Override
        public void run() {
            commitLock.lock();
            try {
                bulkRequest(0);
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                commitLock.unlock();
            }
        }
    }

    private static void test() throws IOException {
        // either use client#prepare, or use Requests# to directly build index/delete requests
        bulkRequestBuilder.add(client.prepareIndex("twitter", "tweet", "1")
                        .setSource(jsonBuilder()
                                        .startObject()
                                        .field("user", "kimchy")
                                        .field("postDate", new Date())
                                        .field("message", "trying out Elasticsearch")
                                        .endObject()
                        )
        );

        bulkRequestBuilder.add(client.prepareIndex("twitter", "tweet", "2")
                        .setSource(jsonBuilder()
                                        .startObject()
                                        .field("user", "kimchy")
                                        .field("postDate", new Date())
                                        .field("message", "another post")
                                        .endObject()
                        )
        );

        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
        }

        for (int i = 0; i < 10; i++) {
            Map<String, Object> json = new HashMap<String, Object>();
            json.put("field", "test");
            addUpdateBuilderToBulk(client.prepareIndex(Config.indexName, Config.typeName, String.valueOf(i)).setSource(jsonBuilder()
                    .startObject()
                    .field("user", "sd")
                    .field("dp", "ds")));
        }
        System.out.println(bulkRequestBuilder.numberOfActions());
    }

    public static void main(String[] args) throws IOException {
        test();
        test();
    }
}
