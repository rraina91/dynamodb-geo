package com.amazonaws.geo;

import com.amazonaws.geo.model.GeoQueryRequest;
import com.amazonaws.services.dynamodbv2.model.*;
import com.dashlabs.dash.geo.s2.internal.S2Manager;
import com.google.common.base.Optional;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

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
            geo.putItemRequest(new PutItemRequest(), 0.0, 0.0, configs);
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
        populatedItem.put("title", new AttributeValue().withS("Ippudo"));
        PutItemRequest request = new PutItemRequest().withTableName(tableName).withItem(populatedItem);
        when(s2Manager.generateGeohash(lat, longitude)).thenReturn(geohash);
        when(s2Manager.generateHashKey(geohash, config.getGeoHashKeyLength())).thenReturn(geohashKey);

        PutItemRequest withGeoProperties = geo.putItemRequest(request, lat, longitude, configs);
        assertNotNull(withGeoProperties);
        assertEquals(request.getTableName(), withGeoProperties.getTableName());
        assertEquals(request.getItem().get("title"), withGeoProperties.getItem().get("title"));
        assertEquals(withGeoProperties.getItem().get(config.getGeoHashColumn()).getN(), String.valueOf(geohash));
        assertEquals(withGeoProperties.getItem().get(config.getGeoHashKeyColumn()).getN(), String.valueOf(geohashKey));
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
        populatedItem.put("title", new AttributeValue().withS("Ippudo"));
        populatedItem.put("venueCategory", new AttributeValue().withS("restaurant"));
        PutItemRequest request = new PutItemRequest().withTableName(tableName).withItem(populatedItem);
        when(s2Manager.generateGeohash(lat, longitude)).thenReturn(geohash);
        when(s2Manager.generateHashKey(geohash, config.getGeoHashKeyLength())).thenReturn(geohashKey);

        PutItemRequest withGeoProperties = geo.putItemRequest(request, lat, longitude, configs);
        assertNotNull(withGeoProperties);
        assertEquals(request.getTableName(), withGeoProperties.getTableName());
        assertEquals(request.getItem().get("title"), withGeoProperties.getItem().get("title"));
        assertEquals(withGeoProperties.getItem().get(config.getGeoHashColumn()).getN(), String.valueOf(geohash));
        assertEquals(withGeoProperties.getItem().get(config.getGeoHashKeyColumn()).getS(), new DefaultHashKeyDecorator().decorate("restaurant", geohashKey));
        verify(s2Manager, times(1)).generateGeohash(lat, longitude);
        verify(s2Manager, times(1)).generateHashKey(geohash, config.getGeoHashKeyLength());
        verifyNoMoreInteractions(s2Manager);

        reset(s2Manager);

        // test where the composite value is null, the composite GSI should not be added to the put-item request
        populatedItem.clear();
        populatedItem.put("title", new AttributeValue().withS("Ippudo"));
        request = new PutItemRequest().withTableName(tableName).withItem(populatedItem);
        when(s2Manager.generateGeohash(lat, longitude)).thenReturn(geohash);
        when(s2Manager.generateHashKey(geohash, config.getGeoHashKeyLength())).thenReturn(geohashKey);

        withGeoProperties = geo.putItemRequest(request, lat, longitude, configs);
        assertNotNull(withGeoProperties);
        assertEquals(request.getTableName(), withGeoProperties.getTableName());
        assertEquals(request.getItem().get("title"), withGeoProperties.getItem().get("title"));
        assertEquals(withGeoProperties.getItem().get(config.getGeoHashColumn()).getN(), String.valueOf(geohash));
        assertNull(withGeoProperties.getItem().get(config.getGeoHashKeyColumn()));
        verify(s2Manager, times(1)).generateGeohash(lat, longitude);
        verify(s2Manager, times(1)).generateHashKey(geohash, config.getGeoHashKeyLength());
        verifyNoMoreInteractions(s2Manager);

    }

    @Test
    public void getItemQueryInvalidFields() {
        Geo geo = new Geo();
        try {
            geo.getItemQuery(new QueryRequest(), 0.0, 0.0, null, null, null, 0, Optional.<String>absent());
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
        Condition expectedGeoHashKeyCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withN(String.valueOf(geohashKey)));
        Condition expectedGeoHashCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withN(String.valueOf(geohash)));

        GeoConfig config = createTestConfig(false, null);
        String tableName = "TableWithSomeData";
        QueryRequest query = new QueryRequest().withTableName(tableName);
        when(s2Manager.generateGeohash(lat, longitude)).thenReturn(geohash);
        when(s2Manager.generateHashKey(geohash, config.getGeoHashKeyLength())).thenReturn(geohashKey);

        QueryRequest withGeoProperties = geo.getItemQuery(query, lat, longitude, config, Optional.<String>absent());
        assertNotNull(withGeoProperties);
        assertEquals(query.getTableName(), withGeoProperties.getTableName());
        assertEquals(withGeoProperties.getKeyConditions().get(config.getGeoHashKeyColumn()), expectedGeoHashKeyCondition);
        assertEquals(withGeoProperties.getKeyConditions().get(config.getGeoHashColumn()), expectedGeoHashCondition);
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
        Condition expectedGeoHashKeyCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withS(String.format("%s:%d", category, geohashKey)));
        Condition expectedGeoHashCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withN(String.valueOf(geohash)));

        GeoConfig config = createTestConfig(true, "category");
        String tableName = "TableWithSomeData";
        QueryRequest query = new QueryRequest().withTableName(tableName);
        when(s2Manager.generateGeohash(lat, longitude)).thenReturn(geohash);
        when(s2Manager.generateHashKey(geohash, config.getGeoHashKeyLength())).thenReturn(geohashKey);

        QueryRequest withGeoProperties = geo.getItemQuery(query, lat, longitude, config, Optional.of(category));
        assertNotNull(withGeoProperties);
        assertEquals(query.getTableName(), withGeoProperties.getTableName());
        assertEquals(withGeoProperties.getKeyConditions().get(config.getGeoHashKeyColumn()), expectedGeoHashKeyCondition);
        assertEquals(withGeoProperties.getKeyConditions().get(config.getGeoHashColumn()), expectedGeoHashCondition);
        verify(s2Manager, times(1)).generateGeohash(lat, longitude);
        verify(s2Manager, times(1)).generateHashKey(geohash, config.getGeoHashKeyLength());
        verifyNoMoreInteractions(s2Manager);
    }

    @Test
    public void radiusQueryInvalidRadius() {
        Geo geo = new Geo();
        try {
            geo.radiusQuery(new QueryRequest(), 0.0, 0.0, -5.0, null, null, null, 0, Optional.<String>absent());
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
        QueryRequest query = new QueryRequest().withTableName(tableName);
        List<QueryRequest> geoQueries = new ArrayList<QueryRequest>();
        geoQueries.add(new QueryRequest().withLimit(100));
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
            geo.rectangleQuery(new QueryRequest(), 0.0, 0.0, 0.0, 0.0, null, null, null, 0, Optional.<String>absent());
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
        QueryRequest query = new QueryRequest().withTableName(tableName);
        List<QueryRequest> geoQueries = new ArrayList<QueryRequest>();
        geoQueries.add(new QueryRequest().withLimit(100));
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
