package com.atmasapps.amazonaws.geo.s2.internal;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import com.atmasapps.amazonaws.geo.model.GeoQueryRequest;
import com.atmasapps.dashlabs.dash.geo.model.filters.GeoFilter;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Created by mpuri on 3/28/14
 */
public class GeoQueryClient {

    /**
     * The db client to use when executing the queries
     */
    private final DynamoDbClient dbClient;

    /**
     * The executor service to use to manage the queries workload
     */
    private final ExecutorService executorService;

    public GeoQueryClient(DynamoDbClient dbClient, ExecutorService executorService) {
        this.dbClient = dbClient;
        this.executorService = executorService;
    }

    /**
     * A convenience method that executes the <code>queryRequests</code> and applies the <code>resultFilter</code> to the query results.
     *
     * @return an immutable collection of filtered items
     */
    public List<Map<String, AttributeValue>> execute(final GeoQueryRequest geoQueryRequest)
            throws InterruptedException, ExecutionException {
        final List<Map<String, AttributeValue>> results = new ArrayList<Map<String, AttributeValue>>();
        List<Future<List<Map<String, AttributeValue>>>> futures;
        final List<Callable<List<Map<String, AttributeValue>>>> queryCallables =
                new ArrayList<Callable<List<Map<String, AttributeValue>>>>(geoQueryRequest.getQueryRequests().size());
        for (final QueryRequest query : geoQueryRequest.getQueryRequests()) {
            queryCallables.add(new Callable<List<Map<String, AttributeValue>>>() {
                @Override public List<Map<String, AttributeValue>> call() throws Exception {
                    return executeQuery(query, geoQueryRequest.getResultFilter());
                }
            });
        }
        futures = executorService.invokeAll(queryCallables);
        if (futures != null) {
            for (Future<List<Map<String, AttributeValue>>> future : futures) {
                results.addAll(future.get());
            }
        }
        return ImmutableList.copyOf(results);
    }

    /**
     * Executes the  query using the provided db client. The geo filter is applied to the results of the query.
     *
     * @param queryRequest the query to execute
     * @return a collection of filtered result items
     */
    private List<Map<String, AttributeValue>> executeQuery(QueryRequest queryRequest, GeoFilter<Map<String, AttributeValue>> resultFilter) {
    	QueryResponse queryResult;
        List<Map<String, AttributeValue>> resultItems = new ArrayList<Map<String, AttributeValue>>();
        do {
            queryResult = dbClient.query(queryRequest);
            List<Map<String, AttributeValue>> items = queryResult.items();
            // filter the results using the geo filter
            List<Map<String, AttributeValue>> filteredItems = resultFilter.filter(items);
            resultItems.addAll(filteredItems);
            queryRequest = queryRequest.toBuilder().exclusiveStartKey(queryResult.lastEvaluatedKey()).build();
        } while ((!queryResult.lastEvaluatedKey().isEmpty()));

        return resultItems;
    }
}
