package com.dashlabs.dash.geo.model.filters;

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * User: blangel
 * Date: 7/19/17
 * Time: 2:22 PM
 */
public class RectangleGeoFilter<T> implements GeoFilter<T> {

    private final GeoDataExtractor<T> extractor;

    /**
     * Bounding box for a rectangle query
     */
    private final S2LatLngRect latLngRect;

    public RectangleGeoFilter(GeoDataExtractor<T> extractor, S2LatLngRect latLngRect) {
        if ((extractor == null) || (latLngRect == null)) {
            throw new IllegalArgumentException();
        }
        this.extractor = extractor;
        this.latLngRect = latLngRect;
    }

    /**
     * Filters out items that are outside the range of the bounding box of this filter.
     *
     * @param items items that need to be filtered.
     * @return result a collection of items that fall within the bounding box of this filter.
     */
    public List<T> filter(Collection<T> items) {
        List<T> result = new ArrayList<>(items.size());
        for (T item : items) {
            Optional<Double> latitude = extractor.extractLatitude(item);
            Optional<Double> longitude = extractor.extractLongitude(item);
            if (latitude.isPresent() && longitude.isPresent()) {
                S2LatLng latLng = S2LatLng.fromDegrees(latitude.get(), longitude.get());
                if (latLngRect.contains(latLng)) {
                    result.add(item);
                }

            }
        }
        return result;
    }
}
