package com.atmasapps.amazonaws.geo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.atmasapps.amazonaws.geo.DefaultHashKeyDecorator;
import com.atmasapps.amazonaws.geo.Geo;
import com.atmasapps.amazonaws.geo.GeoConfig;
import com.atmasapps.amazonaws.geo.GeoQueryHelper;
import com.atmasapps.amazonaws.geo.HashKeyDecorator;
import com.atmasapps.amazonaws.geo.model.GeoQueryRequest;
import com.atmasapps.dashlabs.dash.geo.s2.internal.S2Manager;
import com.google.common.base.Optional;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

/**
 * Created by mpuri on 3/26/14
 */
public class GeoTest {

    @Test
    public void putItemRequestInvalidFields() {
        Geo geo = new Geo();
        try {
            List<GeoConfig> configs = new ArrayList<GeoConfig>();
            configs.add(new GeoConfig(null, null, null, 0, Optional.<HashKeyDecorator>absent(), null));
            geo.putItemRequest(PutItemRequest.builder().build(), 0.0, 0.0, configs);
            fail("Should have failed as there are invalid fields");
        } catch (IllegalArgumentException e) {
            //expected
        }
    }

    @Test
    public void putItemRequest() {
        GeoQueryHelper geoQueryHelper = mock(GeoQueryHelper.class);
        S2Manager s2Manager = mock(S2Manager.class);
        Geo geo = new Geo(s2Manager, geoQueryHelper);
        double lat = 5.0;
        double longitude = -5.5;
        long geohash = System.currentTimeMillis();
        long geohashKey = 12345;
        List<GeoConfig> configs = new ArrayList<GeoConfig>();
        GeoConfig config = createTestConfig(false, null);
        configs.add(config);
        String tableName = "TableWithSomeData";
        Map<String, AttributeValue> populatedItem = new HashMap<String, AttributeValue>();
        populatedItem.put("title", AttributeValue.builder().s("Ippudo").build());
        PutItemRequest request = PutItemRequest.builder().tableName(tableName).item(populatedItem).build();
        when(s2Manager.generateGeohash(lat, longitude)).thenReturn(geohash);
        when(s2Manager.generateHashKey(geohash, config.getGeoHashKeyLength())).thenReturn(geohashKey);

        PutItemRequest withGeoProperties = geo.putItemRequest(request, lat, longitude, configs);
        assertNotNull(withGeoProperties);
        assertEquals(request.tableName(), withGeoProperties.tableName());
        assertEquals(request.item().get("title"), withGeoProperties.item().get("title"));
        assertEquals(withGeoProperties.item().get(config.getGeoHashColumn()).n(), String.valueOf(geohash));
        assertEquals(withGeoProperties.item().get(config.getGeoHashKeyColumn()).n(), String.valueOf(geohashKey));
        verify(s2Manager, times(1)).generateGeohash(lat, longitude);
        verify(s2Manager, times(1)).generateHashKey(geohash, config.getGeoHashKeyLength());
        verifyNoMoreInteractions(s2Manager);
    }

    @Test
    public void putItemRequestWithCompositeColumn() {
        GeoQueryHelper geoQueryHelper = mock(GeoQueryHelper.class);
        S2Manager s2Manager = mock(S2Manager.class);
        Geo geo = new Geo(s2Manager, geoQueryHelper);
        double lat = 5.0;
        double longitude = -5.5;
        long geohash = System.currentTimeMillis();
        long geohashKey = 12345;
        List<GeoConfig> configs = new ArrayList<GeoConfig>();
        GeoConfig config = createTestConfig(true,  "venueCategory");
        configs.add(config);
        String tableName = "TableWithSomeData";
        Map<String, AttributeValue> populatedItem = new HashMap<String, AttributeValue>();
        populatedItem.put("title", AttributeValue.builder().s("Ippudo").build());
        populatedItem.put("venueCategory", AttributeValue.builder().s("restaurant").build());
        PutItemRequest request = PutItemRequest.builder().tableName(tableName).item(populatedItem).build();
        when(s2Manager.generateGeohash(lat, longitude)).thenReturn(geohash);
        when(s2Manager.generateHashKey(geohash, config.getGeoHashKeyLength())).thenReturn(geohashKey);

        PutItemRequest withGeoProperties = geo.putItemRequest(request, lat, longitude, configs);
        assertNotNull(withGeoProperties);
        assertEquals(request.tableName(), withGeoProperties.tableName());
        assertEquals(request.item().get("title"), withGeoProperties.item().get("title"));
        assertEquals(withGeoProperties.item().get(config.getGeoHashColumn()).n(), String.valueOf(geohash));
        assertEquals(withGeoProperties.item().get(config.getGeoHashKeyColumn()).s(), new DefaultHashKeyDecorator().decorate("restaurant", geohashKey));
        verify(s2Manager, times(1)).generateGeohash(lat, longitude);
        verify(s2Manager, times(1)).generateHashKey(geohash, config.getGeoHashKeyLength());
        verifyNoMoreInteractions(s2Manager);

        reset(s2Manager);

        // test where the composite value is null, the composite GSI should not be added to the put-item request
        populatedItem.clear();
        populatedItem.put("title", AttributeValue.builder().s("Ippudo").build());
        request = PutItemRequest.builder().tableName(tableName).item(populatedItem).build();
        when(s2Manager.generateGeohash(lat, longitude)).thenReturn(geohash);
        when(s2Manager.generateHashKey(geohash, config.getGeoHashKeyLength())).thenReturn(geohashKey);

        withGeoProperties = geo.putItemRequest(request, lat, longitude, configs);
        assertNotNull(withGeoProperties);
        assertEquals(request.tableName(), withGeoProperties.tableName());
        assertEquals(request.item().get("title"), withGeoProperties.item().get("title"));
        assertEquals(withGeoProperties.item().get(config.getGeoHashColumn()).n(), String.valueOf(geohash));
        assertNull(withGeoProperties.item().get(config.getGeoHashKeyColumn()));
        verify(s2Manager, times(1)).generateGeohash(lat, longitude);
        verify(s2Manager, times(1)).generateHashKey(geohash, config.getGeoHashKeyLength());
        verifyNoMoreInteractions(s2Manager);

    }

    @Test
    public void getItemQueryInvalidFields() {
        Geo geo = new Geo();
        try {
            geo.getItemQuery(QueryRequest.builder().build(), 0.0, 0.0, null, null, null, 0, Optional.<String>absent());
            fail("Should have failed as there are invalid fields");
        } catch (IllegalArgumentException e) {
            //expected
        }
    }

    @Test
    public void getItemQuery() {
        GeoQueryHelper geoQueryHelper = mock(GeoQueryHelper.class);
        S2Manager s2Manager = mock(S2Manager.class);
        Geo geo = new Geo(s2Manager, geoQueryHelper);
        double lat = 5.0;
        double longitude = -5.5;
        long geohash = System.currentTimeMillis();
        long geohashKey = 12345;
        Condition expectedGeoHashKeyCondition = Condition.builder().comparisonOperator(ComparisonOperator.EQ)
                .attributeValueList(AttributeValue.builder().n(String.valueOf(geohashKey)).build()).build();
        Condition expectedGeoHashCondition = Condition.builder().comparisonOperator(ComparisonOperator.EQ)
                .attributeValueList(AttributeValue.builder().n(String.valueOf(geohash)).build()).build();

        GeoConfig config = createTestConfig(false, null);
        String tableName = "TableWithSomeData";
        QueryRequest query = QueryRequest.builder().tableName(tableName).build();
        when(s2Manager.generateGeohash(lat, longitude)).thenReturn(geohash);
        when(s2Manager.generateHashKey(geohash, config.getGeoHashKeyLength())).thenReturn(geohashKey);

        QueryRequest withGeoProperties = geo.getItemQuery(query, lat, longitude, config, Optional.<String>absent());
        assertNotNull(withGeoProperties);
        assertEquals(query.tableName(), withGeoProperties.tableName());
        assertEquals(withGeoProperties.keyConditions().get(config.getGeoHashKeyColumn()), expectedGeoHashKeyCondition);
        assertEquals(withGeoProperties.keyConditions().get(config.getGeoHashColumn()), expectedGeoHashCondition);
        verify(s2Manager, times(1)).generateGeohash(lat, longitude);
        verify(s2Manager, times(1)).generateHashKey(geohash, config.getGeoHashKeyLength());
        verifyNoMoreInteractions(s2Manager);
    }

    @Test
    public void getItemQueryWithCompositeKey() {
        GeoQueryHelper geoQueryHelper = mock(GeoQueryHelper.class);
        S2Manager s2Manager = mock(S2Manager.class);
        Geo geo = new Geo(s2Manager, geoQueryHelper);
        double lat = 5.0;
        double longitude = -5.5;
        long geohash = System.currentTimeMillis();
        long geohashKey = 12345;
        String category = "restaurant";
        Condition expectedGeoHashKeyCondition = Condition.builder().comparisonOperator(ComparisonOperator.EQ)
                .attributeValueList(AttributeValue.builder().s(String.format("%s:%d", category, geohashKey)).build()).build();
        Condition expectedGeoHashCondition = Condition.builder().comparisonOperator(ComparisonOperator.EQ)
                .attributeValueList(AttributeValue.builder().n(String.valueOf(geohash)).build()).build();

        GeoConfig config = createTestConfig(true, "category");
        String tableName = "TableWithSomeData";
        QueryRequest query = QueryRequest.builder().tableName(tableName).build();
        when(s2Manager.generateGeohash(lat, longitude)).thenReturn(geohash);
        when(s2Manager.generateHashKey(geohash, config.getGeoHashKeyLength())).thenReturn(geohashKey);

        QueryRequest withGeoProperties = geo.getItemQuery(query, lat, longitude, config, Optional.of(category));
        assertNotNull(withGeoProperties);
        assertEquals(query.tableName(), withGeoProperties.tableName());
        assertEquals(withGeoProperties.keyConditions().get(config.getGeoHashKeyColumn()), expectedGeoHashKeyCondition);
        assertEquals(withGeoProperties.keyConditions().get(config.getGeoHashColumn()), expectedGeoHashCondition);
        verify(s2Manager, times(1)).generateGeohash(lat, longitude);
        verify(s2Manager, times(1)).generateHashKey(geohash, config.getGeoHashKeyLength());
        verifyNoMoreInteractions(s2Manager);
    }

    @Test
    public void radiusQueryInvalidRadius() {
        Geo geo = new Geo();
        try {
            geo.radiusQuery(QueryRequest.builder().build(), 0.0, 0.0, -5.0, null, null, null, 0, Optional.<String>absent());
            fail("Should have failed as there are invalid fields");
        } catch (IllegalArgumentException e) {
            //expected
        }
    }

    @Test
    public void radiusQuery() {
        GeoQueryHelper geoQueryHelper = mock(GeoQueryHelper.class);
        S2Manager s2Manager = mock(S2Manager.class);
        Geo geo = new Geo(s2Manager, geoQueryHelper);
        double lat = 5.0;
        double longitude = -5.5;
        double radius = 50;
        String category = "restaurant";
        GeoConfig config = createTestConfig(true, "category");
        String tableName = "TableWithSomeData";
        QueryRequest query = QueryRequest.builder().tableName(tableName).build();
        List<QueryRequest> geoQueries = new ArrayList<QueryRequest>();
        geoQueries.add(QueryRequest.builder().limit(100).build());
        S2LatLngRect latLngRect = new S2LatLngRect(S2LatLng.fromDegrees(lat, longitude), S2LatLng.fromDegrees(lat + 10, longitude + 10));
        when(s2Manager.getBoundingBoxForRadiusQuery(lat, longitude, radius)).thenReturn(latLngRect);
        when(geoQueryHelper.generateGeoQueries(query, latLngRect, config, Optional.of(category))).thenReturn(geoQueries);
        GeoQueryRequest geoQueryRequest = geo.radiusQuery(query, lat, longitude, radius, config, Optional.of(category));
        assertNotNull(geoQueryRequest);
        assertNotNull(geoQueryRequest.getResultFilter());
        assertNotNull(geoQueryRequest.getQueryRequests());
        assertEquals(geoQueryRequest.getQueryRequests(), geoQueries);
        verify(s2Manager, times(1)).getBoundingBoxForRadiusQuery(lat, longitude, radius);
        verify(geoQueryHelper, times(1)).generateGeoQueries(query, latLngRect, config, Optional.of(category));
        verifyNoMoreInteractions(s2Manager, geoQueryHelper);
    }

    @Test
    public void rectangleQueryInvalidFields() {
        Geo geo = new Geo();
        try {
            geo.rectangleQuery(QueryRequest.builder().build(), 0.0, 0.0, 0.0, 0.0, null, null, null, 0, Optional.<String>absent());
            fail("Should have failed as there are invalid fields");
        } catch (IllegalArgumentException e) {
            //expected
        }
    }

    @Test
    public void rectangleQuery() {
        GeoQueryHelper geoQueryHelper = mock(GeoQueryHelper.class);
        S2Manager s2Manager = mock(S2Manager.class);
        Geo geo = new Geo(s2Manager, geoQueryHelper);
        double minLat = 5.0;
        double minLongitude = -5.5;
        double maxLat = 15.0;
        double maxLongitude = -25.5;
        GeoConfig config = createTestConfig(false, null);
        String tableName = "TableWithSomeData";
        QueryRequest query = QueryRequest.builder().tableName(tableName).build();
        List<QueryRequest> geoQueries = new ArrayList<QueryRequest>();
        geoQueries.add(QueryRequest.builder().limit(100).build());
        S2LatLngRect latLngRect = new S2LatLngRect(S2LatLng.fromDegrees(minLat, minLongitude), S2LatLng.fromDegrees(maxLat, maxLongitude));
        when(s2Manager.getBoundingBoxForRectangleQuery(minLat, minLongitude, maxLat, maxLongitude)).thenReturn(latLngRect);
        when(geoQueryHelper.generateGeoQueries(query, latLngRect, config, Optional.<String>absent())).thenReturn(geoQueries);
        GeoQueryRequest geoQueryRequest = geo.rectangleQuery(query, minLat, minLongitude, maxLat, maxLongitude, config, Optional.<String>absent());
        assertNotNull(geoQueryRequest);
        assertNotNull(geoQueryRequest.getResultFilter());
        assertNotNull(geoQueryRequest.getQueryRequests());
        assertEquals(geoQueryRequest.getQueryRequests(), geoQueries);
        verify(s2Manager, times(1)).getBoundingBoxForRectangleQuery(minLat, minLongitude, maxLat, maxLongitude);
        verify(geoQueryHelper, times(1)).generateGeoQueries(query, latLngRect, config, Optional.<String>absent());
        verifyNoMoreInteractions(s2Manager, geoQueryHelper);
    }

    private GeoConfig createTestConfig(boolean withKeyDecorator, String compositeColumnName) {
        int hashKeyLength = 3;
        String geoIndexName = "VenueGeoIndex";
        String geoHashKeyColumn = "geoHashKey";
        String geoHashColumn = "geohash";

        GeoConfig.Builder builder = new GeoConfig.Builder().geoIndexName(geoIndexName).geoHashKeyLength(hashKeyLength)
                .geoHashKeyColumn(geoHashKeyColumn).geoHashColumn(geoHashColumn);
        if(withKeyDecorator) {
            HashKeyDecorator decorator = new DefaultHashKeyDecorator();
            builder.hashKeyDecorator(Optional.of(decorator));
            builder.compositeHashKeyColumn(Optional.of(compositeColumnName));
        }else{
            builder.hashKeyDecorator(Optional.<HashKeyDecorator>absent());
        }
        return builder.build();
    }

}
