package com.atmasapps.amazonaws.geo.model;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

import java.util.List;
import java.util.Map;

import com.atmasapps.dashlabs.dash.geo.model.filters.GeoFilter;

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
