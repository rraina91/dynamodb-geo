package com.amazonaws.geo.model.filters;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by mpuri on 3/26/14.
 */
public class RectangleGeoFilter implements GeoFilter {

    /**
     * Bounding box for a rectangle query
     */
    private final S2LatLngRect latLngRect;

    public RectangleGeoFilter(S2LatLngRect latLngRect) {
        this.latLngRect = latLngRect;
    }

    public S2LatLngRect getLatLngRect() {
        return latLngRect;
    }

    /**
     * Filters out items that are outside the range of the bounding box of this filter.
     *
     * @param items items that need to be filtered.
     * @return result a collection of items that fall within the bounding box of this filter.
     */
    public List<Map<String, AttributeValue>> filter(List<Map<String, AttributeValue>> items) {
        List<Map<String, AttributeValue>> result = new ArrayList<Map<String, AttributeValue>>();
        for (Map<String, AttributeValue> item : items) {
            if ((item.get(LATITUDE_FIELD) != null) && (item.get(LATITUDE_FIELD).getN() != null)
                    && (item.get(LONGITUDE_FIELD) != null) && (item.get(LONGITUDE_FIELD).getN() != null)) {
                S2LatLng latLng = S2LatLng.fromDegrees(Double.valueOf(item.get(LATITUDE_FIELD).getN()), Double.valueOf(item.get(LONGITUDE_FIELD).getN()));
                if (latLngRect != null && latLngRect.contains(latLng)) {
                    result.add(item);
                }

            }
        }
        return result;
    }
}
