package com.dashlabs.dash.geo.model.filters;

import com.google.common.geometry.S2LatLng;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 *
 * Originally created by mpuri on 3/26/14
 *
 * User: blangel
 * Date: 7/19/17
 * Time: 2:11 PM
 *
 * Modified to abstract the data type.
 */
public class RadiusGeoFilter<T> implements GeoFilter<T> {

    private final GeoDataExtractor<T> extractor;

    /**
     * Represents a center point as a lat/long - used by radius queries
     */
    private final S2LatLng centerLatLng;

    /**
     * Radius(in metres)
     */
    private final double radiusInMeter;

    public RadiusGeoFilter(GeoDataExtractor<T> extractor, S2LatLng centerLatLng, double radiusInMeter) {
        if ((extractor == null) || (centerLatLng == null) || (radiusInMeter <= 0)) {
            throw new IllegalArgumentException();
        }
        this.extractor = extractor;
        this.centerLatLng = centerLatLng;
        this.radiusInMeter = radiusInMeter;
    }

    /**
     * Filters out items that are outside the range of the radius of this filter.
     *
     * @param items items that need to be filtered.
     * @return result a collection of items that fall within the radius of this filter.
     */
    public List<T> filter(Collection<T> items) {
        List<T> result = new ArrayList<>(items.size());
        for (T item : items) {
            Optional<Double> latitude = extractor.extractLatitude(item);
            Optional<Double> longitude = extractor.extractLongitude(item);
            if (latitude.isPresent() && longitude.isPresent()) {
                S2LatLng latLng = S2LatLng.fromDegrees(latitude.get(), longitude.get());
                if (centerLatLng.getEarthDistance(latLng) <= radiusInMeter) {
                    result.add(item);
                }
            }
        }
        return result;
    }

}
