package com.amazonaws.geo.model.filters;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.List;
import java.util.Map;

/**
 * Created by mpuri on 3/26/14.
 * Represents a filter that can be applied to a collection of items and return a subset of those items.
 */
public interface GeoFilter {

    /**
     * Fields required for Geo querying
     */
     static final String LATITUDE_FIELD = "latitude";

     static final String LONGITUDE_FIELD = "longitude";

    /**
     * Filters out entities from the given list of <code>items</code>
     * @param items a list of items that need to be filtered
     * @return filteredItems a list containing only the remaining items that did not get filtered.
     */
    List<Map<String, AttributeValue>> filter(List<Map<String, AttributeValue>> items);
}
