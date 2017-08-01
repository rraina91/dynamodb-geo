package com.dashlabs.dash.geo.s3.model;

/**
 * User: blangel
 * Date: 8/1/17
 * Time: 8:34 AM
 */
public class GeoProperties {

    private final int hashKeyLength;

    private final long geoHashKey;

    private final double latitude;

    private final double longitude;

    public GeoProperties(int hashKeyLength, long geoHashKey, double latitude, double longitude) {
        this.hashKeyLength = hashKeyLength;
        this.geoHashKey = geoHashKey;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public int getHashKeyLength() {
        return hashKeyLength;
    }

    public long getGeoHashKey() {
        return geoHashKey;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public String getHashKeyLengthAsString() {
        return String.valueOf(hashKeyLength);
    }

    public String getGeohashKeyAsString() {
        return String.valueOf(geoHashKey);
    }

    public String getLatitudeAsString() {
        return String.valueOf(latitude);
    }

    public String getLongitudeAsString() {
        return String.valueOf(longitude);
    }
}
