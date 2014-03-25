package com.amazonaws.geo;

/**
 * Created by mpuri on 3/24/14.
 */
public class GeoConfig {

    /**
     * The index name of the global secondary index that exists on a table for GeoSpatial querying.
     * It comprises of the geoHashKeyColumn(hashKey) and geoHashColumn(range key).
     */
    private final String geoIndexName;

    /**
     * The hashKey of the global secondary index used for GeoSpatial querying.
     * It's value is derived from the <code>geoHashColumn</code> in conjunction with the <code>geoHashKeyLength</code>
     */
    private final String geoHashKeyColumn;

    /**
     * The column containing the geoHash for a lat/long pair.
     * It is mapped as the range key in the global secondary index used for GeoSpatial querying.
     */
    private final String geoHashColumn;

    /**
     * The size of the hashKey used in the global secondary index used for GeoSpatial querying.
     */
    private final int geoHashKeyLength;

    /**
     * The column containing a string representation of the item's lat/long.
     */
    private final String latLongColumn;

    public GeoConfig(String geoIndexName, String geoHashKeyColumn, String geoHashColumn, int geoHashKeyLength, String latLongColumn) {
        this.geoIndexName = geoIndexName;
        this.geoHashKeyColumn = geoHashKeyColumn;
        this.geoHashColumn = geoHashColumn;
        this.geoHashKeyLength = geoHashKeyLength;
        this.latLongColumn = latLongColumn;
    }

    public String getGeoIndexName() {
        return geoIndexName;
    }

    public String getGeoHashKeyColumn() {
        return geoHashKeyColumn;
    }

    public String getGeoHashColumn() {
        return geoHashColumn;
    }

    public int getGeoHashKeyLength() {
        return geoHashKeyLength;
    }

    public String getLatLongColumn() {
        return latLongColumn;
    }

    /**
     * Builder to help with the construction of a <code>GeoConfig</code>
     */
    public static class Builder {
        private String geoIndexName;
        private String geoHashKeyColumn;
        private String geoHashColumn;
        private int geoHashKeyLength;
        private String latLongColumn;

        public Builder() {

        }

        public Builder geoIndexName(String geoIndexName) {
            this.geoIndexName = geoIndexName;
            return this;
        }

        public Builder geoHashKeyColumn(String geoHashKeyColumn) {
            this.geoHashKeyColumn = geoHashKeyColumn;
            return this;
        }

        public Builder geoHashColumn(String geoHashColumn) {
            this.geoHashColumn = geoHashColumn;
            return this;
        }

        public Builder geoHashKeyLength(int geoHashKeyLength) {
            this.geoHashKeyLength = geoHashKeyLength;
            return this;
        }

        public Builder latLongColumn(String latLongColumn) {
            this.latLongColumn = latLongColumn;
            return this;
        }

        public GeoConfig build() {
            return new GeoConfig(this.geoIndexName, this.geoHashKeyColumn, this.geoHashColumn, this.geoHashKeyLength, this.latLongColumn);
        }

    }
}
