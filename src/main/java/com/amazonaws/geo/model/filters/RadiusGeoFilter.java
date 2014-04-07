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

    /**
     * Column containing the item's lat/long as a string - used for reverse lookup.
     */
    private final String latLongColumn;

    public RadiusGeoFilter(S2LatLng centerLatLng, double radiusInMeter, String latLongColumn) {
        this.centerLatLng = centerLatLng;
        this.radiusInMeter = radiusInMeter;
        this.latLongColumn = latLongColumn;
    }

    public S2LatLng getCenterLatLng() {
        return centerLatLng;
    }

    public double getRadiusInMeter() {
        return radiusInMeter;
    }

    public String getLatLongColumn() {
        return latLongColumn;
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
            String latLongStr = item.get(latLongColumn).getS();
            if (latLongStr != null) {
                String[] latLong = latLongStr.split(",");
                if (latLong.length == 2) {
                    S2LatLng latLng = S2LatLng.fromDegrees(Double.valueOf(latLong[0]), Double.valueOf(latLong[1]));
                    if (centerLatLng != null && radiusInMeter > 0
                            && centerLatLng.getEarthDistance(latLng) <= radiusInMeter) {
                        result.add(item);
                    }
                }
            }
        }
        return result;
    }
}
