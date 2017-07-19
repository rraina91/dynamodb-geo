package com.amazonaws.geo.model;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.dashlabs.dash.geo.model.filters.GeoFilter;

import java.util.List;
import java.util.Map;

/**
 * Created by mpuri on 3/25/14.
 * A wrapper that encapsulates the collection of queries that are generated for a radius or a rectangle query
 * and the filter that has to be applied to the query results.
 */
public class GeoQueryRequest {

    private final List<QueryRequest> queryRequests;

    private final GeoFilter<Map<String, AttributeValue>> resultFilter;

    public GeoQueryRequest(List<QueryRequest> queryRequests, GeoFilter<Map<String, AttributeValue>> resultFilter) {
        this.queryRequests = queryRequests;
        this.resultFilter = resultFilter;
    }

    public List<QueryRequest> getQueryRequests() {
        return queryRequests;
    }

    public GeoFilter<Map<String, AttributeValue>> getResultFilter() {
        return resultFilter;
    }

}
