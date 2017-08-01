package com.dashlabs.dash.geo.s3.model.filters;

import com.dashlabs.dash.geo.model.filters.GeoDataExtractor;
import com.dashlabs.dash.geo.model.filters.GeoFilter;
import com.dashlabs.dash.geo.model.filters.RadiusGeoFilter;
import com.dashlabs.dash.geo.model.filters.RectangleGeoFilter;
import com.dashlabs.dash.geo.s3.model.GeoProperties;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;

import java.util.Optional;

/**
 * User: blangel
 * Date: 8/1/17
 * Time: 8:46 AM
 */
public class GeoFilters {

    private static final GeoDataExtractor<GeoProperties> EXTRACTOR = new GeoDataExtractor<GeoProperties>() {
        @Override public Optional<Double> extractLatitude(GeoProperties item) {
            if (item == null) {
                return Optional.empty();
            }
            return Optional.of(item.getLatitude());
        }

        @Override public Optional<Double> extractLongitude(GeoProperties item) {
            if (item == null) {
                return Optional.empty();
            }
            return Optional.of(item.getLongitude());
        }
    };

    /**
     * Factory method to create a filter used by radius queries.
     *
     * @param centerLatLng  the lat/long of the center of the filter's radius
     * @param radiusInMeter the radius of the filter in metres
     * @return a new instance of the {@link RadiusGeoFilter}
     */
    public static GeoFilter<GeoProperties> newRadiusFilter(S2LatLng centerLatLng, double radiusInMeter) {
        return com.dashlabs.dash.geo.model.filters.GeoFilters.newRadiusFilter(EXTRACTOR, centerLatLng, radiusInMeter);
    }

    /**
     * Factory method to create a filter used by rectangle queries
     *
     * @param latLngRect the bounding box for the filter
     * @return a new instance of the {@link RectangleGeoFilter}
     */
    public static GeoFilter<GeoProperties> newRectangleFilter(S2LatLngRect latLngRect) {
        return com.dashlabs.dash.geo.model.filters.GeoFilters.newRectangleFilter(EXTRACTOR, latLngRect);
    }

}
