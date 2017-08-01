package com.amazonaws.geo;

/**
 * Created by mpuri on 4/14/14
 */
public class DefaultHashKeyDecorator implements HashKeyDecorator {

    @Override public String decorate(String columnValue, long geoHashKey) {
        return String.format("%s:%d", columnValue, geoHashKey);
    }
}
