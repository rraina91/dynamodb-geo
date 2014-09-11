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

    /**
     * Column containing the item's lat/long as a string - used for reverse lookup.
     */
    private final String latLongColumn;

    public RectangleGeoFilter(S2LatLngRect latLngRect, String latLongColumn) {
        this.latLngRect = latLngRect;
        this.latLongColumn = latLongColumn;
    }

    public S2LatLngRect getLatLngRect() {
        return latLngRect;
    }

    public String getLatLongColumn() {
        return latLongColumn;
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
            if (item.get(latLongColumn) != null && item.get(latLongColumn).getS() != null) {
                String latLongStr = item.get(latLongColumn).getS();
                String[] latLong = latLongStr.split(",");
                if (latLong.length == 2) {
                    S2LatLng latLng = S2LatLng.fromDegrees(Double.valueOf(latLong[0]), Double.valueOf(latLong[1]));
                    if (latLngRect != null && latLngRect.contains(latLng)) {
                        result.add(item);
                    }
                }

            }
        }
        return result;
    }
}
