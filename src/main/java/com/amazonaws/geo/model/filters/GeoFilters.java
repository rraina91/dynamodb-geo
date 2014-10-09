package com.amazonaws.geo.model.filters;

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;

/**
 * Created by mpuri on 3/26/14.
 * Factory methods for {@link GeoFilter}.
 */
public class GeoFilters {

    /**
     * Factory method to create a filter used by radius queries.
     *
     * @param centerLatLng      the lat/long of the center of the filter's radius
     * @param radiusInMeter     the radius of the filter in metres
     * @return a new instance of the {@link RadiusGeoFilter}
     */
    public static GeoFilter newRadiusFilter(S2LatLng centerLatLng, double radiusInMeter) {
        return new RadiusGeoFilter(centerLatLng, radiusInMeter);
    }

    /**
     * Factory method to create a filter used by rectangle queries
     *
     * @param latLngRect the bounding box for the filter
     * @return a new instance of the {@link RectangleGeoFilter}
     */
    public static GeoFilter newRectangleFilter(S2LatLngRect latLngRect) {
        return new RectangleGeoFilter(latLngRect);
    }

}
