package com.amazonaws.geo;

import com.amazonaws.geo.model.GeoQueryRequest;
import com.amazonaws.geo.model.filters.RadiusGeoFilter;
import com.amazonaws.geo.model.filters.RectangleGeoFilter;
import com.amazonaws.geo.s2.internal.S2Manager;
import com.amazonaws.services.dynamodbv2.model.*;
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
 * Created by mpuri on 3/26/14.
 */
public class GeoTest {

    @Test
    public void putItemRequestInvalidFields() {
        Geo geo = new Geo();
        try {
            geo.putItemRequest(new PutItemRequest(), 0.0, 0.0, null, null, null, 0, null);
            fail("Should have failed as there are invalid fields");
        } catch (IllegalArgumentException e) {
            //expected
        }
    }

    @Test
    public void putItemRequest() {
        GeoQueryHelper geoQueryHelper = mock(GeoQueryHelper.class);
        S2Manager s2Manager = mock(S2Manager.class);
        Geo geo = new Geo(geoQueryHelper, s2Manager);
        double lat = 5.0;
        double longitude = -5.5;
        long geohash = System.currentTimeMillis();
        long geohashKey = 12345;

        GeoConfig config = createTestConfig();
        String tableName = "TableWithSomeData";
        Map<String, AttributeValue> populatedItem = new HashMap<String, AttributeValue>();
        populatedItem.put("title", new AttributeValue().withS("Ippudo"));
        PutItemRequest request = new PutItemRequest().withTableName(tableName).withItem(populatedItem);
        when(s2Manager.generateGeohash(lat, longitude)).thenReturn(geohash);
        when(s2Manager.generateHashKey(geohash, config.getGeoHashKeyLength())).thenReturn(geohashKey);

        PutItemRequest withGeoProperties = geo.putItemRequest(request, lat, longitude, config);
        assertNotNull(withGeoProperties);
        assertEquals(request.getTableName(), withGeoProperties.getTableName());
        assertEquals(request.getItem().get("title"), withGeoProperties.getItem().get("title"));
        assertEquals(withGeoProperties.getItem().get(config.getGeoHashColumn()).getN(), String.valueOf(geohash));
        assertEquals(withGeoProperties.getItem().get(config.getGeoHashKeyColumn()).getN(), String.valueOf(geohashKey));
        assertNotNull(withGeoProperties.getItem().get(config.getLatLongColumn()).getS());
        verify(s2Manager, times(1)).generateGeohash(lat, longitude);
        verify(s2Manager, times(1)).generateHashKey(geohash, config.getGeoHashKeyLength());
        verifyNoMoreInteractions(s2Manager);
    }

    @Test
    public void getItemQueryInvalidFields() {
        Geo geo = new Geo();
        try {
            geo.getItemQuery(new QueryRequest(), 0.0, 0.0, null, null, null, 0, null);
            fail("Should have failed as there are invalid fields");
        } catch (IllegalArgumentException e) {
            //expected
        }
    }

    @Test
    public void getItemQuery() {
        GeoQueryHelper geoQueryHelper = mock(GeoQueryHelper.class);
        S2Manager s2Manager = mock(S2Manager.class);
        Geo geo = new Geo(geoQueryHelper, s2Manager);
        double lat = 5.0;
        double longitude = -5.5;
        long geohash = System.currentTimeMillis();
        long geohashKey = 12345;
        Condition expectedGeoHashKeyCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withN(String.valueOf(geohashKey)));
        Condition expectedGeoHashCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withN(String.valueOf(geohash)));

        GeoConfig config = createTestConfig();
        String tableName = "TableWithSomeData";
        QueryRequest query = new QueryRequest().withTableName(tableName);
        when(s2Manager.generateGeohash(lat, longitude)).thenReturn(geohash);
        when(s2Manager.generateHashKey(geohash, config.getGeoHashKeyLength())).thenReturn(geohashKey);

        QueryRequest withGeoProperties = geo.getItemQuery(query, lat, longitude, config);
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
            geo.radiusQuery(new QueryRequest(), 0.0, 0.0, -5.0, null, null, null, 0, null);
            fail("Should have failed as there are invalid fields");
        } catch (IllegalArgumentException e) {
            //expected
        }
    }

    @Test
    public void radiusQuery() {
        GeoQueryHelper geoQueryHelper = mock(GeoQueryHelper.class);
        S2Manager s2Manager = mock(S2Manager.class);
        Geo geo = new Geo(geoQueryHelper, s2Manager);
        double lat = 5.0;
        double longitude = -5.5;
        double radius = 50;
        GeoConfig config = createTestConfig();
        String tableName = "TableWithSomeData";
        QueryRequest query = new QueryRequest().withTableName(tableName);
        List<QueryRequest> geoQueries = new ArrayList<QueryRequest>();
        geoQueries.add(new QueryRequest().withLimit(100));
        S2LatLngRect latLngRect = new S2LatLngRect(S2LatLng.fromDegrees(lat, longitude), S2LatLng.fromDegrees(lat + 10, longitude + 10));
        when(s2Manager.getBoundingBoxForRadiusQuery(lat, longitude, radius)).thenReturn(latLngRect);
        when(geoQueryHelper.generateGeoQueries(query, latLngRect, config)).thenReturn(geoQueries);
        GeoQueryRequest geoQueryRequest = geo.radiusQuery(query, lat, longitude, radius, config);
        assertNotNull(geoQueryRequest);
        assertNotNull(geoQueryRequest.getResultFilter());
        assertNotNull(((RadiusGeoFilter)geoQueryRequest.getResultFilter()).getCenterLatLng());
        assertNotNull(((RadiusGeoFilter)geoQueryRequest.getResultFilter()).getRadiusInMeter());
        assertNotNull(geoQueryRequest.getQueryRequests());
        assertEquals(geoQueryRequest.getQueryRequests(), geoQueries);
        verify(s2Manager, times(1)).getBoundingBoxForRadiusQuery(lat, longitude, radius);
        verify(geoQueryHelper, times(1)).generateGeoQueries(query, latLngRect, config);
        verifyNoMoreInteractions(s2Manager, geoQueryHelper);
    }

    @Test
    public void rectangleQueryInvalidFields() {
        Geo geo = new Geo();
        try {
            geo.rectangleQuery(new QueryRequest(), 0.0, 0.0, 0.0, 0.0, null, null, null, 0, null);
            fail("Should have failed as there are invalid fields");
        } catch (IllegalArgumentException e) {
            //expected
        }
    }

    @Test
    public void rectangleQuery() {
        GeoQueryHelper geoQueryHelper = mock(GeoQueryHelper.class);
        S2Manager s2Manager = mock(S2Manager.class);
        Geo geo = new Geo(geoQueryHelper, s2Manager);
        double minLat = 5.0;
        double minLongitude = -5.5;
        double maxLat = 15.0;
        double maxLongitude = -25.5;
        GeoConfig config = createTestConfig();
        String tableName = "TableWithSomeData";
        QueryRequest query = new QueryRequest().withTableName(tableName);
        List<QueryRequest> geoQueries = new ArrayList<QueryRequest>();
        geoQueries.add(new QueryRequest().withLimit(100));
        S2LatLngRect latLngRect = new S2LatLngRect(S2LatLng.fromDegrees(minLat, minLongitude), S2LatLng.fromDegrees(maxLat, maxLongitude));
        when(s2Manager.getBoundingBoxForRectangleQuery(minLat, minLongitude, maxLat, maxLongitude)).thenReturn(latLngRect);
        when(geoQueryHelper.generateGeoQueries(query, latLngRect, config)).thenReturn(geoQueries);
        GeoQueryRequest geoQueryRequest = geo.rectangleQuery(query, minLat, minLongitude, maxLat, maxLongitude, config);
        assertNotNull(geoQueryRequest);
        assertNotNull(geoQueryRequest.getResultFilter());
        assertNotNull(((RectangleGeoFilter) geoQueryRequest.getResultFilter()).getLatLngRect());
        assertNotNull(geoQueryRequest.getQueryRequests());
        assertEquals(geoQueryRequest.getQueryRequests(), geoQueries);
        verify(s2Manager, times(1)).getBoundingBoxForRectangleQuery(minLat, minLongitude, maxLat, maxLongitude);
        verify(geoQueryHelper, times(1)).generateGeoQueries(query, latLngRect, config);
        verifyNoMoreInteractions(s2Manager, geoQueryHelper);
    }

    private GeoConfig createTestConfig() {
        int hashKeyLength = 3;
        String geoIndexName = "VenueGeoIndex";
        String geoHashKeyColumn = "geoHashKey";
        String geoHashColumn = "geohash";
        String latLongColumn = "latLong";
        return new GeoConfig.Builder().latLongColumn(latLongColumn).geoIndexName(geoIndexName).geoHashKeyLength(hashKeyLength)
                .geoHashKeyColumn(geoHashKeyColumn).geoHashColumn(geoHashColumn).build();
    }

}
