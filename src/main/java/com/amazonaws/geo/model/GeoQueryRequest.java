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
    private final List<QueryRequest> queryRequests;
    private final GeoFilter resultFilter;

    public GeoQueryRequest(List<QueryRequest> queryRequests, GeoFilter resultFilter) {
        this.queryRequests = queryRequests;
        this.resultFilter = resultFilter;
    }

    public List<QueryRequest> getQueryRequests() {
        return queryRequests;
    }

    public GeoFilter getResultFilter() {
        return resultFilter;
    }

}
