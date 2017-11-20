package com.dashlabs.dash.geo.s3;

import com.dashlabs.dash.geo.s3.model.GeoProperties;
import com.dashlabs.dash.geo.model.filters.GeoFilter;
import com.dashlabs.dash.geo.s2.internal.S2Manager;
import com.dashlabs.dash.geo.s3.model.filters.GeoFilters;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;

import java.util.Collection;
import java.util.List;

/**
 * User: blangel
 * Date: 7/20/17
 * Time: 9:03 AM
 */
public class Geo {

    private final S2Manager s2Manager;

    private final GeoQueryHelper helper;

    public Geo() {
        this(new S2Manager());
    }

    private Geo(S2Manager s2Manager) {
        this(s2Manager, new GeoQueryHelper(s2Manager));
    }

    protected Geo(S2Manager s2Manager, GeoQueryHelper helper) {
        this.s2Manager = s2Manager;
        this.helper = helper;
    }

    public GeoProperties getGeoProperties(int geoHashLength, double latitude, double longitude) {
        long geohash = s2Manager.generateGeohash(latitude, longitude);
        long geohashKey = s2Manager.generateHashKey(geohash, geoHashLength);
        return new GeoProperties(geoHashLength, geohashKey, latitude, longitude);
    }

    public List<GeoProperties> generatePropertiesForRadiusQuery(int geoHashLength, double latitude, double longitude, double radius) {
        S2LatLngRect boundingBox = s2Manager.getBoundingBoxForRadiusQuery(latitude, longitude, radius);
        return helper.generateGeoProperties(boundingBox, geoHashLength);
    }

    public List<GeoProperties> generatePropertiesForRectangleQuery(int geoHashLength, double minLatitude, double minLongitude,
                                                                   double maxLatitude, double maxLongitude) {
        S2LatLngRect boundingBox = s2Manager.getBoundingBoxForRectangleQuery(minLatitude, minLongitude, maxLatitude, maxLongitude);
        return helper.generateGeoProperties(boundingBox, geoHashLength);
    }

    public List<GeoProperties> filterByRadius(Collection<GeoProperties> properties, double latitude, double longitude, double radius) {
        S2LatLng centerLatLng = S2LatLng.fromDegrees(latitude, longitude);
        GeoFilter<GeoProperties> filter = GeoFilters.newRadiusFilter(centerLatLng, radius);
        return filter.filter(properties);
    }

    public List<GeoProperties> filterByRectangle(Collection<GeoProperties> properties, double minLatitude, double minLongitude,
                                                 double maxLatitude, double maxLongitude) {
        S2LatLngRect boundingBox = s2Manager.getBoundingBoxForRectangleQuery(minLatitude, minLongitude, maxLatitude, maxLongitude);
        GeoFilter<GeoProperties> filter = GeoFilters.newRectangleFilter(boundingBox);
        return filter.filter(properties);
    }

}
