package com.dashlabs.dash.geo.model.filters;

import java.util.Collection;
import java.util.List;

/**
 *
 * Created originally by mpuri on 3/26/14.
 * Represents a filter that can be applied to a collection of items and return a subset of those items.
 *
 * User: blangel
 * Date: 7/19/17
 * Time: 2:09 PM
 *
 * Modified original to abstract the data type
 */
public interface GeoFilter<T> {

    /**
     * Fields required for Geo querying
     */
    static final String LATITUDE_FIELD = "latitude";

    static final String LONGITUDE_FIELD = "longitude";

    /**
     * Filters out entities from the given list of <code>items</code>
     *
     * @param items a list of items that need to be filtered
     * @return filteredItems a list containing only the remaining items that did not get filtered.
     */
    List<T> filter(Collection<T> items);

}
