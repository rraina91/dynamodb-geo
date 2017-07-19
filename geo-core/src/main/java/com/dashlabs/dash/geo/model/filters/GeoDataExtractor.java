package com.dashlabs.dash.geo.model.filters;

import java.util.Optional;

/**
 * User: blangel
 * Date: 7/19/17
 * Time: 2:10 PM
 */
public interface GeoDataExtractor<T> {

    Optional<Double> extractLatitude(T item);

    Optional<Double> extractLongitude(T item);

}
