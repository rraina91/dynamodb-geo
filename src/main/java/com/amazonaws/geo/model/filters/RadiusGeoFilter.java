package com.amazonaws.geo.model.filters;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.geometry.S2LatLng;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by mpuri on 3/26/14.
 */
public class RadiusGeoFilter implements GeoFilter {

    /**
     * Represents a center point as a lat/long - used by radius queries
     */
    private final S2LatLng centerLatLng;

    /**
     * Radius(in metres)
     */
    private final double radiusInMeter;

    public RadiusGeoFilter(S2LatLng centerLatLng, double radiusInMeter) {
        this.centerLatLng = centerLatLng;
        this.radiusInMeter = radiusInMeter;
    }

    public S2LatLng getCenterLatLng() {
        return centerLatLng;
    }

    public double getRadiusInMeter() {
        return radiusInMeter;
    }

    /**
     * Filters out items that are outside the range of the radius of this filter.
     *
     * @param items items that need to be filtered.
     * @return result a collection of items that fall within the radius of this filter.
     */
    public List<Map<String, AttributeValue>> filter(List<Map<String, AttributeValue>> items) {
        List<Map<String, AttributeValue>> result = new ArrayList<Map<String, AttributeValue>>();
        for (Map<String, AttributeValue> item : items) {
            if ((item.get(LATITUDE_FIELD) != null) && (item.get(LATITUDE_FIELD).getN() != null)
                    && (item.get(LONGITUDE_FIELD) != null) && (item.get(LONGITUDE_FIELD).getN() != null)) {
                S2LatLng latLng = S2LatLng.fromDegrees(Double.valueOf(item.get(LATITUDE_FIELD).getN()), Double.valueOf(
                        item.get(LONGITUDE_FIELD).getN()));
                if (centerLatLng != null && radiusInMeter > 0
                        && centerLatLng.getEarthDistance(latLng) <= radiusInMeter) {
                    result.add(item);
                }
            }
        }
        return result;
    }
}
