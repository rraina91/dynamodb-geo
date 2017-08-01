package com.dashlabs.dash.geo.s3;

import com.dashlabs.dash.geo.AbstractGeoQueryHelper;
import com.dashlabs.dash.geo.model.GeohashRange;
import com.dashlabs.dash.geo.s2.internal.S2Manager;
import com.dashlabs.dash.geo.s3.model.GeoProperties;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.geometry.S2CellUnion;
import com.google.common.geometry.S2LatLngRect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: blangel
 * Date: 8/1/17
 * Time: 9:06 AM
 */
public class GeoQueryHelper extends AbstractGeoQueryHelper {

    public GeoQueryHelper(S2Manager s2Manager) {
        super(s2Manager);
    }

    /**
     * For the given <code>QueryRequest</code> query and the boundingBox, this method creates a collection of queries
     * that are decorated with geo attributes to enable geo-spatial querying.
     *
     * @param boundingBox the bounding lat long rectangle of the geo query
     * @param hashKeyLength the hash key length for the geo query
     * @return an immutable collection of {@linkplain GeoProperties}
     */
    public List<GeoProperties> generateGeoProperties(S2LatLngRect boundingBox, int hashKeyLength) {
        List<GeohashRange> outerRanges = getGeoHashRanges(boundingBox);
        List<GeoProperties> queryRequests = new ArrayList<GeoProperties>(outerRanges.size());
        //Create multiple queries based on the geo ranges derived from the bounding box
        for (GeohashRange outerRange : outerRanges) {
            List<GeohashRange> geohashRanges = outerRange.trySplit(hashKeyLength, s2Manager);
            for (GeohashRange range : geohashRanges) {
                long geoHashKey = s2Manager.generateHashKey(range.getRangeMin(), hashKeyLength);
                queryRequests.add(new GeoProperties(hashKeyLength, geoHashKey, range.getRangeMin(), range.getRangeMax()));
            }
        }
        return ImmutableList.copyOf(queryRequests);
    }

}
