package com.amazonaws.geo;

import com.amazonaws.geo.model.*;
import com.amazonaws.geo.model.filters.GeoFilter;
import com.amazonaws.geo.model.filters.GeoFilters;
import com.amazonaws.geo.s2.internal.S2Manager;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by mpuri on 3/24/14.
 */
public class Geo {

    private final GeoQueryHelper geoQueryHelper;
    private final S2Manager s2Manager;

    public Geo() {
        this.s2Manager = new S2Manager();
        this.geoQueryHelper = new GeoQueryHelper(s2Manager);
    }

    public Geo(GeoQueryHelper geoQueryHelper, S2Manager s2Manager) {
        this.geoQueryHelper = geoQueryHelper;
        this.s2Manager = s2Manager;
    }

    /**
     * Decorates the given <code>putItemRequest</code> with attributes required for geo spatial querying.
     *
     * @param putItemRequest   the request that needs to be decorated with geo attributes
     * @param latitude         the latitude that needs to be attached with the item
     * @param longitude        the longitude that needs to be attached with the item
     * @param geoIndexName     name of the global secondary index for geo spatial querying
     * @param geoHashKeyColumn name of the column that stores the item's geoHashKey. This column is used as a hash key of the global secondary index
     * @param geoHashColumn    name of the column that stores the item's geohash. This column is used as a range key in the global secondary index
     * @param geoHashKeyLength the length of the geohashKey. GeoHashKey is a substring of the item's geohash
     * @param latLongColumn    name of the column that stores the item's lat/long as a string representation.
     * @return the decorated request
     */
    public PutItemRequest putItemRequest(PutItemRequest putItemRequest, double latitude, double longitude, String geoIndexName,
                                         String geoHashKeyColumn,
                                         String geoHashColumn,
                                         int geoHashKeyLength, String latLongColumn) {
        GeoConfig config = new GeoConfig.Builder().geoHashColumn(geoHashColumn).geoHashKeyColumn(geoHashKeyColumn).geoHashKeyLength(
                geoHashKeyLength).geoIndexName(geoIndexName).latLongColumn(latLongColumn).build();
        return putItemRequest(putItemRequest, latitude, longitude, config);
    }

    /**
     * Decorates the given <code>putItemRequest</code> with attributes required for geo spatial querying.
     *
     * @param putItemRequest the request that needs to be decorated with geo attributes
     * @param latitude       the latitude that needs to be attached with the item
     * @param longitude      the longitude that needs to be attached with the item
     * @param config         the configuration to be used for decorating the request with geo attributes
     * @return the decorated request
     */
    public PutItemRequest putItemRequest(PutItemRequest putItemRequest, double latitude, double longitude, GeoConfig config) {
        //Fail-fast if any of the preconditions fail
        checkConfigParams(config.getGeoIndexName(), config.getGeoHashKeyColumn(), config.getGeoHashColumn(), config.getGeoHashKeyLength(),
                config.getLatLongColumn());

        long geohash = s2Manager.generateGeohash(latitude, longitude);
        long geoHashKey = s2Manager.generateHashKey(geohash, config.getGeoHashKeyLength());
        String geoJson = latLongStr(latitude, longitude);

        //Decorate the request with the geohash
        AttributeValue geoHashValue = new AttributeValue().withN(Long.toString(geohash));
        putItemRequest.getItem().put(config.getGeoHashColumn(), geoHashValue);

        //Decorate the request with the geoHashKey
        AttributeValue geoHashKeyValue = new AttributeValue().withN(String.valueOf(geoHashKey));
        putItemRequest.getItem().put(config.getGeoHashKeyColumn(), geoHashKeyValue);

        //Decorate the request with a json representation of the lat/long
        AttributeValue geoJsonValue = new AttributeValue().withS(geoJson);
        putItemRequest.getItem().put(config.getLatLongColumn(), geoJsonValue);

        return putItemRequest;
    }

    /**
     * Decorates the given query request with attributes required for geo spatial querying.
     *
     * @param queryRequest the request that needs to be decorated with geo attributes
     * @param latitude     the latitude of the item that is being queried
     * @param longitude    the longitude of the item that is being queried
     * @param config       the configuration to be used for decorating the request with geo attributes
     * @return the decorated request
     */
    public QueryRequest getItemQuery(QueryRequest queryRequest, double latitude, double longitude, GeoConfig config) {
        checkConfigParams(config.getGeoIndexName(), config.getGeoHashKeyColumn(), config.getGeoHashColumn(), config.getGeoHashKeyLength(),
                config.getLatLongColumn());

        //Generate the geohash and geoHashKey to query by global secondary index
        long geohash = s2Manager.generateGeohash(latitude, longitude);
        long geoHashKey = s2Manager.generateHashKey(geohash, config.getGeoHashKeyLength());
        queryRequest.withIndexName(config.getGeoIndexName());
        Map<String, Condition> keyConditions = new HashMap<String, Condition>();

        //Construct the geohashKey condition
        Condition geoHashKeyCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withN(String.valueOf(geoHashKey)));
        keyConditions.put(config.getGeoHashKeyColumn(), geoHashKeyCondition);

        //Construct the geohash condition
        Condition geoHashCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withN(String.valueOf(geohash)));
        keyConditions.put(config.getGeoHashColumn(), geoHashCondition);

        queryRequest.setKeyConditions(keyConditions);
        return queryRequest;
    }

    /**
     * Decorates the given query request with attributes required for geo spatial querying.
     *
     * @param queryRequest     the request that needs to be decorated with geo attributes
     * @param latitude         the latitude of the item that is being queried
     * @param longitude        the longitude of the item that is being queried
     * @param geoIndexName     name of the global secondary index for geo spatial querying
     * @param geoHashKeyColumn name of the column that stores the item's geoHashKey. This column is used as a hash key of the global secondary index
     * @param geoHashColumn    name of the column that stores the item's geohash. This column is used as a range key in the global secondary index
     * @param geoHashKeyLength the length of the geohashKey. GeoHashKey is a substring of the item's geohash
     * @param latLongColumn    name of the column that stores the item's lat/long as a string representation.
     * @return the decorated request
     */
    public QueryRequest getItemQuery(QueryRequest queryRequest, double latitude, double longitude, String geoIndexName,
                                     String geoHashKeyColumn,
                                     String geoHashColumn,
                                     int geoHashKeyLength, String latLongColumn) {
        GeoConfig config = new GeoConfig.Builder().geoHashColumn(geoHashColumn).geoHashKeyColumn(geoHashKeyColumn).geoHashKeyLength(
                geoHashKeyLength).geoIndexName(geoIndexName).latLongColumn(latLongColumn).build();
        return getItemQuery(queryRequest, latitude, longitude, config);
    }

    /**
     * Creates a wrapper that contains a collection of all queries that are generated as a result of the radius query.
     * It also contains a filter {@link com.amazonaws.geo.model.filters.GeoFilter} that needs to be applied to the results of the query
     * to ensure that everything is in the radius.
     * This is needed because queries are fired for every cell that intersects with the radius' rectangle box.
     *
     * @param queryRequest the request that needs to be decorated with geo attributes
     * @param latitude     the latitude of the center point for the radius query
     * @param longitude    the longitude of the center point for the radius query
     * @param radius       the radius (in metres)
     * @param config       the configuration to be used for decorating the request with geo attributes
     * @return the wrapper containing the generated queries and the geo filter
     */
    public GeoQueryRequest radiusQuery(QueryRequest queryRequest, double latitude, double longitude, double radius, GeoConfig config) {
        checkArgument(radius >= 0.0, "radius has to be a positive value: %s", radius);
        checkConfigParams(config.getGeoIndexName(), config.getGeoHashKeyColumn(), config.getGeoHashColumn(), config.getGeoHashKeyLength(),
                config.getLatLongColumn());
        //Center latLong is needed for the radius filter
        S2LatLng centerLatLng = S2LatLng.fromDegrees(latitude, longitude);
        GeoFilter filter = GeoFilters.newRadiusFilter(centerLatLng, radius, config.getLatLongColumn());
        //Bounding box is needed to generate queries for each cell that intersects with the bounding box
        S2LatLngRect boundingBox = s2Manager.getBoundingBoxForRadiusQuery(latitude, longitude, radius);
        List<QueryRequest> geoQueries = geoQueryHelper.generateGeoQueries(queryRequest, boundingBox, config);
        return new GeoQueryRequest(geoQueries, filter);
    }

    /**
     * Creates a wrapper that contains a collection of all queries that are generated as a result of the radius query.
     * It also contains a filter {@link com.amazonaws.geo.model.filters.GeoFilter} that needs to be applied to the results of the query
     * to ensure that everything is in the radius.
     * This is needed because queries are fired for every cell that intersects with the radius' rectangle box.
     *
     * @param queryRequest     the request that needs to be decorated with geo attributes
     * @param latitude         the latitude of the center point for the radius query
     * @param longitude        the longitude of the center point for the radius query
     * @param radius           the radius (in metres)
     * @param geoIndexName     name of the global secondary index for geo spatial querying
     * @param geoHashKeyColumn name of the column that stores the item's geoHashKey. This column is used as a hash key of the global secondary index
     * @param geoHashColumn    name of the column that stores the item's geohash. This column is used as a range key in the global secondary index
     * @param geoHashKeyLength the length of the geohashKey. GeoHashKey is a substring of the item's geohash
     * @param latLongColumn    name of the column that stores the item's lat/long as a string representation.
     * @return the wrapper containing the generated queries and the geo filter
     */
    public GeoQueryRequest radiusQuery(QueryRequest queryRequest, double latitude, double longitude, double radius, String geoIndexName,
                                       String geoHashKeyColumn,
                                       String geoHashColumn,
                                       int geoHashKeyLength, String latLongColumn) {
        GeoConfig config = new GeoConfig.Builder().geoHashColumn(geoHashColumn).geoHashKeyColumn(geoHashKeyColumn).geoHashKeyLength(
                geoHashKeyLength).geoIndexName(geoIndexName).latLongColumn(latLongColumn).build();
        return radiusQuery(queryRequest, latitude, longitude, radius, config);
    }

    /**
     * Creates a wrapper that contains a collection of all queries that are generated as a result of this rectangle query.
     * It also contains a filter {@link com.amazonaws.geo.model.filters.GeoFilter} that needs to be applied to the results of the query
     * to ensure that everything is in the bounding box of the queried rectangle.
     * This is needed because queries are fired for every cell that intersects with the rectangle's bounding box.
     *
     * @param queryRequest the request that needs to be decorated with geo attributes
     * @param minLatitude  the latitude of the min point of the rectangle
     * @param minLongitude the longitude of the min point of the rectangle
     * @param maxLatitude  the latitude of the max point of the rectangle
     * @param maxLongitude the longitude of the max point of the rectangle
     * @param config       the configuration to be used for decorating the request with geo attributes
     * @return the wrapper containing the generated queries and the geo filter
     */
    public GeoQueryRequest rectangleQuery(QueryRequest queryRequest, double minLatitude, double minLongitude, double maxLatitude,
                                          double maxLongitude, GeoConfig config) {
        checkConfigParams(config.getGeoIndexName(), config.getGeoHashKeyColumn(), config.getGeoHashColumn(), config.getGeoHashKeyLength(),
                config.getLatLongColumn());
        // bounding box is needed for the filter and to generate the queries
        // for each cell that intersects with the bounding box
        S2LatLngRect boundingBox = s2Manager.getBoundingBoxForRectangleQuery(minLatitude, minLongitude, maxLatitude, maxLongitude);
        GeoFilter filter = GeoFilters.newRectangleFilter(boundingBox, config.getLatLongColumn());
        List<QueryRequest> geoQueries = geoQueryHelper.generateGeoQueries(queryRequest, boundingBox, config);
        return new GeoQueryRequest(geoQueries, filter);
    }

    /**
     * Creates a wrapper that contains a collection of all queries that are generated as a result of this rectangle query.
     * It also contains a filter {@link com.amazonaws.geo.model.filters.GeoFilter} that needs to be applied to the results of the query
     * to ensure that everything is in the bounding box of the queried rectangle.
     * This is needed because queries are fired for every cell that intersects with the rectangle's bounding box.
     *
     * @param queryRequest     the request that needs to be decorated with geo attributes
     * @param minLatitude      the latitude of the min point of the rectangle
     * @param minLongitude     the longitude of the min point of the rectangle
     * @param maxLatitude      the latitude of the max point of the rectangle
     * @param maxLongitude     the longitude of the max point of the rectangle
     * @param geoIndexName     name of the global secondary index for geo spatial querying
     * @param geoHashKeyColumn name of the column that stores the item's geoHashKey. This column is used as a hash key of the global secondary index
     * @param geoHashColumn    name of the column that stores the item's geohash. This column is used as a range key in the global secondary index
     * @param geoHashKeyLength the length of the geohashKey. GeoHashKey is a substring of the item's geohash
     * @param latLongColumn    name of the column that stores the item's lat/long as a string representation.
     * @return the wrapper containing the generated queries and the geo filter
     */
    public GeoQueryRequest rectangleQuery(QueryRequest queryRequest, double minLatitude, double minLongitude, double maxLatitude,
                                          double maxLongitude, String geoIndexName,
                                          String geoHashKeyColumn,
                                          String geoHashColumn,
                                          int geoHashKeyLength, String latLongColumn) {
        GeoConfig config = new GeoConfig.Builder().geoHashColumn(geoHashColumn).geoHashKeyColumn(geoHashKeyColumn).geoHashKeyLength(
                geoHashKeyLength).geoIndexName(geoIndexName).latLongColumn(latLongColumn).build();
        return rectangleQuery(queryRequest, minLatitude, minLongitude, maxLatitude, maxLongitude, config);
    }

    /**
     * Checks the values of the geo config
     *
     * @param geoIndexName     name of the global secondary index for geo spatial querying
     * @param geoHashKeyColumn name of the column that stores the item's geoHashKey. This column is used as a hash key of the global secondary index
     * @param geoHashColumn    name of the column that stores the item's geohash. This column is used as a range key in the global secondary index
     * @param geoHashKeyLength the length of the geohashKey. GeoHashKey is a substring of the item's geohash
     * @param latLongColumn    name of the column that stores the item's lat/long as a string representation.
     */
    private void checkConfigParams(String geoIndexName, String geoHashKeyColumn, String geoHashColumn, int geoHashKeyLength,
                                   String latLongColumn) {
        checkArgument((geoIndexName != null && geoIndexName.length() > 0), "geoIndexName cannot be empty: %s", geoIndexName);
        checkArgument((geoHashKeyColumn != null && geoHashKeyColumn.length() > 0), "geoHashKeyColumn cannot be empty: %s",
                geoHashKeyColumn);
        checkArgument((geoHashColumn != null && geoHashColumn.length() > 0), "geoHashColumn cannot be empty: %s", geoHashColumn);
        checkArgument(geoHashKeyLength > 0, "geoHashKeyLength must be a positive number: %s", String.valueOf(geoHashKeyLength));
        checkArgument(latLongColumn != null && latLongColumn.length() > 0, "latLongColumn cannot be empty: %s",
                String.valueOf(latLongColumn));

    }

    /**
     * Creates a string representation of the lat/long
     *
     * @param latitude  the latitude
     * @param longitude the longitude
     * @return string representation of the lat/long.
     */
    private String latLongStr(double latitude, double longitude) {
        return String.format("%f,%f", latitude, longitude);
    }

}
