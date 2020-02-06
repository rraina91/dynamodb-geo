package com.atmasapps.amazonaws.geo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

import com.atmasapps.amazonaws.geo.model.GeoQueryRequest;
import com.atmasapps.amazonaws.geo.s2.internal.GeoQueryClient;
import com.atmasapps.dashlabs.dash.geo.model.filters.GeoFilter;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

/**
 * Created by mpuri on 3/26/14
 */
public class GeoQueryClientTest {

    @Test @SuppressWarnings("unchecked")
    public void execute() {
        DynamoDbClient dbClient = mock(DynamoDbClient.class);
        GeoFilter<Map<String, AttributeValue>> geoFilter = mock(GeoFilter.class);
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        GeoQueryClient geoQueryClient = new GeoQueryClient(dbClient,executorService);

        //Mock queries that get fired by the execute method
        List<QueryRequest> queryRequests = new ArrayList<QueryRequest>();
        QueryRequest query1 = QueryRequest.builder().limit(5).build();
        QueryRequest query2 = QueryRequest.builder().limit(10).build();
        queryRequests.add(query1);
        queryRequests.add(query2);

        //Mock results of the first query
        QueryResponse.Builder result1 = QueryResponse.builder();
        List<Map<String, AttributeValue>> resultItems1 = new ArrayList<Map<String, AttributeValue>>();
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>();
        item1.put("title", AttributeValue.builder().s("Milk Bar").build());
        item1.put("tag", AttributeValue.builder().s("cafe:breakfast:american").build());

        Map<String, AttributeValue> item2 = new HashMap<String, AttributeValue>();
        item2.put("title", AttributeValue.builder().s("Chuko").build());
        item2.put("tag", AttributeValue.builder().s("restaurant:noodles:japanese").build());
        resultItems1.add(item1);
        resultItems1.add(item2);
        result1.items(resultItems1);

        //Mock results of the second query
        QueryResponse.Builder result2 = QueryResponse.builder();
        List<Map<String, AttributeValue>> resultItems2 = new ArrayList<Map<String, AttributeValue>>();
        Map<String, AttributeValue> item3 = new HashMap<String, AttributeValue>();
        item3.put("title", AttributeValue.builder().s("Al Di La").build());
        item3.put("tag", AttributeValue.builder().s("restaurant:italian").build());

        Map<String, AttributeValue> item4 = new HashMap<String, AttributeValue>();
        item4.put("title", AttributeValue.builder().s("Blue Print").build());
        item4.put("tag", AttributeValue.builder().s("bar:cocktails").build());
        resultItems2.add(item3);
        resultItems2.add(item4);
        result2.items(resultItems2);

        when(dbClient.query(query1)).thenReturn(result1.build());
        when(dbClient.query(query2)).thenReturn(result2.build());
        when(geoFilter.filter(resultItems1)).thenReturn(resultItems1);
        List<Map<String, AttributeValue>> filteredList = new ArrayList<Map<String, AttributeValue>>();
        filteredList.add(resultItems2.get(0));
        when(geoFilter.filter(resultItems2)).thenReturn(filteredList);

        GeoQueryRequest geoQueryRequest = new GeoQueryRequest(queryRequests, geoFilter);
        try {
            List<Map<String, AttributeValue>> results = geoQueryClient.execute(geoQueryRequest);
            assertNotNull(results);
            assertEquals(results.size(), 3);
        } catch (InterruptedException | ExecutionException ie) {
            fail("error occurred while executing the queries");
        } finally {
            executorService.shutdown();
        }
    }
}
