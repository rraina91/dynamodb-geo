package com.atmasapps.amazonaws.geo.model.filters;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import com.atmasapps.dashlabs.dash.geo.model.filters.GeoDataExtractor;
import com.atmasapps.dashlabs.dash.geo.model.filters.GeoFilter;
import com.atmasapps.dashlabs.dash.geo.model.filters.RadiusGeoFilter;
import com.atmasapps.dashlabs.dash.geo.model.filters.RectangleGeoFilter;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;

import java.util.Map;
import java.util.Optional;

/**
 * Created by mpuri on 3/26/14.
 * Factory methods for {@link GeoFilter}.
 */
public class GeoFilters {

    private static final GeoDataExtractor<Map<String, AttributeValue>> EXTRACTOR = new GeoDataExtractor<Map<String, AttributeValue>>() {
        @Override public Optional<Double> extractLatitude(Map<String, AttributeValue> item) {
            if ((item.get(GeoFilter.LATITUDE_FIELD) != null) && (item.get(GeoFilter.LATITUDE_FIELD).n() != null)) {
                return Optional.of(Double.valueOf(item.get(GeoFilter.LATITUDE_FIELD).n()));
            }
            return Optional.empty();
        }

        @Override public Optional<Double> extractLongitude(Map<String, AttributeValue> item) {
            if ((item.get(GeoFilter.LONGITUDE_FIELD) != null) && (item.get(GeoFilter.LONGITUDE_FIELD).n() != null)) {
                return Optional.of(Double.valueOf(item.get(GeoFilter.LONGITUDE_FIELD).n()));
            }
            return Optional.empty();
        }
    };

    /**
     * Factory method to create a filter used by radius queries.
     *
     * @param centerLatLng      the lat/long of the center of the filter's radius
     * @param radiusInMeter     the radius of the filter in metres
     * @return a new instance of the {@link RadiusGeoFilter}
     */
    public static GeoFilter<Map<String, AttributeValue>> newRadiusFilter(S2LatLng centerLatLng, double radiusInMeter) {
        return com.atmasapps.dashlabs.dash.geo.model.filters.GeoFilters.newRadiusFilter(EXTRACTOR, centerLatLng, radiusInMeter);
    }

    /**
     * Factory method to create a filter used by rectangle queries
     *
     * @param latLngRect the bounding box for the filter
     * @return a new instance of the {@link RectangleGeoFilter}
     */
    public static GeoFilter<Map<String, AttributeValue>> newRectangleFilter(S2LatLngRect latLngRect) {
        return com.atmasapps.dashlabs.dash.geo.model.filters.GeoFilters.newRectangleFilter(EXTRACTOR, latLngRect);
    }

}
