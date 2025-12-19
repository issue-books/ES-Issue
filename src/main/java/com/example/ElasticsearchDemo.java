package com.example;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Conflicts;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteByQueryRequest;
import co.elastic.clients.elasticsearch.core.DeleteByQueryResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ElasticsearchDemo {

    public static void main(String[] args) {
        // Create the low-level client
        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200)).build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // Create the API client
        ElasticsearchClient client = new ElasticsearchClient(transport);
        ElasticsearchAsyncClient asyncClient = new ElasticsearchAsyncClient(transport);

        try {
            // 1. Prepare Data
            prepareData(client);

            // 2. Reproduce Issue (Async)
            // reproduceIssue(asyncClient);

            // 2. Reproduce Issue (Sync Concurrent)
            reproduceIssueSyncConcurrent(client);

            // Wait a bit for async operations to complete/fail
            Thread.sleep(5000);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                restClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void prepareData(ElasticsearchClient client) throws IOException {
        System.out.println("Preparing data...");
        BulkRequest.Builder br = new BulkRequest.Builder();

        for (int i = 0; i < 1000; i++) {
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("user", "kimchy");
            jsonMap.put("message", "trying out Elasticsearch " + i);
            
            String id = String.valueOf(i);
            br.operations(op -> op
                .index(idx -> idx
                    .index("users")
                    .id(id)
                    .document(jsonMap)
                )
            );
        }
        
        // Execute bulk request
        BulkResponse result = client.bulk(br.refresh(Refresh.True).build());
        
        if (result.errors()) {
            System.err.println("Bulk had errors");
        } else {
            System.out.println("Data prepared.");
        }
    }

    private static void reproduceIssueSyncConcurrent(ElasticsearchClient client) throws InterruptedException {
        System.out.println("Starting Sync Concurrent reproduction loop...");
        int loopCount = 3;
        CountDownLatch latch = new CountDownLatch(loopCount);

        for (int i = 0; i < loopCount; i++) {
            final int loopIndex = i + 1;
            new Thread(() -> {
                System.out.println("Loop " + loopIndex + ": Initiating delete_by_query (Sync)...");
                try {
                    DeleteByQueryRequest request = new DeleteByQueryRequest.Builder()
                            .index("users")
                            .query(q -> q
                                    .term(t -> t
                                            .field("user")
                                            .value("kimchy")
                                    )
                            )
//                            .conflicts(Conflicts.Proceed)  // 解决方案：忽略版本冲突
                            .build();

                    DeleteByQueryResponse response = client.deleteByQuery(request);
                    System.out.println("Loop " + loopIndex + ": Success. Deleted " + response.deleted());
                } catch (Exception e) {
                    System.err.println("Loop " + loopIndex + ": Failed. " + e.getMessage());
                    if (e.getMessage().contains("Conflict") || e.getMessage().contains("409")) {
                        System.err.println("Loop " + loopIndex + ": 409 Conflict Reproduced!");
                    }
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await(10, TimeUnit.SECONDS);
    }

    private static void reproduceIssue(ElasticsearchAsyncClient client) throws InterruptedException {
        System.out.println("Starting reproduction loop...");
        int loopCount = 3;
        CountDownLatch latch = new CountDownLatch(loopCount);

        for (int i = 0; i < loopCount; i++) {
            final int loopIndex = i + 1;
            System.out.println("Loop " + loopIndex + ": Initiating delete_by_query...");
            
            DeleteByQueryRequest request = new DeleteByQueryRequest.Builder()
                    .index("users")
                    .query(q -> q
                        .term(t -> t
                            .field("user")
                            .value("kimchy")
                        )
                    )
                    .build();

            CompletableFuture<DeleteByQueryResponse> future = client.deleteByQuery(request);
            
            future.whenComplete((response, exception) -> {
                if (exception != null) {
                    System.err.println("Loop " + loopIndex + ": Failed. " + exception.getMessage());
                    if (exception.getMessage().contains("Conflict") || exception.getMessage().contains("409")) {
                         System.err.println("Loop " + loopIndex + ": 409 Conflict Reproduced!");
                    }
                } else {
                    System.out.println("Loop " + loopIndex + ": Success. Deleted " + response.deleted());
                }
                latch.countDown();
            });
        }
        
        latch.await(10, TimeUnit.SECONDS);
    }
}
