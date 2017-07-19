package com.amazonaws.geo;

import com.amazonaws.geo.model.GeoQueryRequest;
import com.amazonaws.geo.s2.internal.GeoQueryClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.dashlabs.dash.geo.model.filters.GeoFilter;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by mpuri on 3/26/14
 */
public class GeoQueryClientTest {

    @Test @SuppressWarnings("unchecked")
    public void execute() {
        AmazonDynamoDBClient dbClient = mock(AmazonDynamoDBClient.class);
        GeoFilter<Map<String, AttributeValue>> geoFilter = mock(GeoFilter.class);
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        GeoQueryClient geoQueryClient = new GeoQueryClient(dbClient,executorService);

        //Mock queries that get fired by the execute method
        List<QueryRequest> queryRequests = new ArrayList<QueryRequest>();
        QueryRequest query1 = new QueryRequest().withLimit(5);
        QueryRequest query2 = new QueryRequest().withLimit(10);
        queryRequests.add(query1);
        queryRequests.add(query2);

        //Mock results of the first query
        QueryResult result1 = new QueryResult();
        List<Map<String, AttributeValue>> resultItems1 = new ArrayList<Map<String, AttributeValue>>();
        Map<String, AttributeValue> item1 = new HashMap<String, AttributeValue>();
        item1.put("title", new AttributeValue().withS("Milk Bar"));
        item1.put("tag", new AttributeValue().withS("cafe:breakfast:american"));

        Map<String, AttributeValue> item2 = new HashMap<String, AttributeValue>();
        item2.put("title", new AttributeValue().withS("Chuko"));
        item2.put("tag", new AttributeValue().withS("restaurant:noodles:japanese"));
        resultItems1.add(item1);
        resultItems1.add(item2);
        result1.setItems(resultItems1);

        //Mock results of the second query
        QueryResult result2 = new QueryResult();
        List<Map<String, AttributeValue>> resultItems2 = new ArrayList<Map<String, AttributeValue>>();
        Map<String, AttributeValue> item3 = new HashMap<String, AttributeValue>();
        item3.put("title", new AttributeValue().withS("Al Di La"));
        item3.put("tag", new AttributeValue().withS("restaurant:italian"));

        Map<String, AttributeValue> item4 = new HashMap<String, AttributeValue>();
        item4.put("title", new AttributeValue().withS("Blue Print"));
        item4.put("tag", new AttributeValue().withS("bar:cocktails"));
        resultItems2.add(item3);
        resultItems2.add(item4);
        result2.setItems(resultItems2);

        when(dbClient.query(query1)).thenReturn(result1);
        when(dbClient.query(query2)).thenReturn(result2);
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
