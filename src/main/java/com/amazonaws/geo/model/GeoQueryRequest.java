package com.amazonaws.geo.model;

import com.amazonaws.geo.model.filters.GeoFilter;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Created by mpuri on 3/25/14.
 * A wrapper that encapsulates the collection of queries that are generated for a radius or a rectangle query
 * and the filter that has to be applied to the query results.
 */
public class GeoQueryRequest {
    private static final Logger LOG = LoggerFactory.getLogger(GeoQueryRequest.class);
    private final List<QueryRequest> queryRequests;
    private final GeoFilter resultFilter;

    public GeoQueryRequest(List<QueryRequest> queryRequests, GeoFilter resultFilter) {
        this.queryRequests = queryRequests;
        this.resultFilter = resultFilter;
    }

    /**
     * A convenience method that executes the <code>queryRequests</code> and applies the <code>resultFilter</code> to the query results.
     *
     * @param dbClient        the db client to use when executing the queries
     * @param executorService the executor service to use to manage the queries workload
     * @return a collection of filtered items
     */
    public List<Map<String, AttributeValue>> execute(final AmazonDynamoDBClient dbClient, final ExecutorService executorService)
            throws InterruptedException, ExecutionException {
        final List<Map<String, AttributeValue>> results = new ArrayList<Map<String, AttributeValue>>();
        List<Future<List<Map<String, AttributeValue>>>> futures;
        final List<Callable<List<Map<String, AttributeValue>>>> queryCallables =
                new ArrayList<Callable<List<Map<String, AttributeValue>>>>();
        for (final QueryRequest query : queryRequests) {
            queryCallables.add(new Callable<List<Map<String, AttributeValue>>>() {
                @Override public List<Map<String, AttributeValue>> call() throws Exception {
                    return executeQuery(dbClient, query, resultFilter);
                }
            });
        }
        futures = executorService.invokeAll(queryCallables);
        if (futures != null) {
            for (Future<List<Map<String, AttributeValue>>> future : futures) {
                results.addAll(future.get());
            }
        }
        return results;
    }

    /**
     * Executes the  query using the provided db client. The geo filter is applied to the results of the query.
     *
     * @param dbClient     the database client to use
     * @param queryRequest the query to execute
     * @param resultFilter the geo filter to apply on the query results
     * @return a collection of filtered result items
     */
    private List<Map<String, AttributeValue>> executeQuery(AmazonDynamoDBClient dbClient, QueryRequest queryRequest, GeoFilter resultFilter
    ) {
        QueryResult queryResult;
        List<Map<String, AttributeValue>> resultItems = new ArrayList<Map<String, AttributeValue>>();
        do {
            try {
                queryResult = dbClient.query(queryRequest);
            } catch (ResourceNotFoundException rnfe) {
                // queryRequest not found, simply break
                break;
            }
            List<Map<String, AttributeValue>> items = queryResult.getItems();
            // filter the results using the geo filter
            List<Map<String, AttributeValue>> filteredItems = resultFilter.filter(items);
            resultItems.addAll(filteredItems);
            queryRequest = queryRequest.withExclusiveStartKey(queryResult.getLastEvaluatedKey());
        } while ((queryResult.getLastEvaluatedKey() != null));

        return resultItems;
    }

    public List<QueryRequest> getQueryRequests() {
        return queryRequests;
    }

    public GeoFilter getResultFilter() {
        return resultFilter;
    }

}
