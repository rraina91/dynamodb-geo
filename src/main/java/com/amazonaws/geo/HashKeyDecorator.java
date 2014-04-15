package com.amazonaws.geo;

/**
 * Created by mpuri on 4/14/14.
 * This decorator is used to create a composite geoHashKey. You can use a composite geoHashKey to fire
 * geo queries that also take into consideration another column.
 * For eg. "Fetch an item where lat/long is 23.78787, -70.6767 AND category = 'restaurants'.
 */
public interface HashKeyDecorator {

    /**
     * Creates a composite hashKey used for geo querying.
     * @param columnValue the value of the column that needs to be part of the hashKey
     * @param geoHashKey the geoHashKey of the item
     * @return a string containing the geoHashKey and the additional column value
     */
    String decorate(String columnValue, long geoHashKey);

}
