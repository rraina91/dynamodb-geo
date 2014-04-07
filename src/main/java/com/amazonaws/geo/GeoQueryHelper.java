package com.amazonaws.geo;

import com.amazonaws.geo.model.GeohashRange;
import com.amazonaws.geo.s2.internal.S2Manager;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2CellUnion;
import com.google.common.geometry.S2LatLngRect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mpuri on 3/25/14.
 */
public class GeoQueryHelper {

    private final S2Manager s2Manager;

    public GeoQueryHelper(S2Manager s2Manager) {
        this.s2Manager = s2Manager;
    }

    /**
     * For the given <code>QueryRequest</code> query and the boundingBox, this method creates a collection of queries
     * that are decorated with geo attributes to enable geo-spatial querying.
     *
     * @param query       the original query request
     * @param boundingBox the bounding lat long rectangle of the geo query
     * @param config      the config containing caller's geo config, example index name, etc.
     * @return queryRequests an immutable collection of <code>QueryRequest</code> that are now "geo enabled"
     */
    public List<QueryRequest> generateGeoQueries(QueryRequest query, S2LatLngRect boundingBox, GeoConfig config) {
        List<GeohashRange> outerRanges = getGeoHashRanges(boundingBox);
        List<QueryRequest> queryRequests = new ArrayList<QueryRequest>(outerRanges.size());
        //Create multiple queries based on the geo ranges derived from the bounding box
        for (GeohashRange outerRange : outerRanges) {
            List<GeohashRange> geohashRanges = outerRange.trySplit(config.getGeoHashKeyLength(), s2Manager);
            for (GeohashRange range : geohashRanges) {
                //Make a copy of the query request to retain original query attributes like table name, etc.
                QueryRequest queryRequest = copyQueryRequest(query);

                //generate the hash key for the global secondary index
                long geohashKey = s2Manager.generateHashKey(range.getRangeMin(), config.getGeoHashKeyLength());
                Map<String, Condition> keyConditions = new HashMap<String, Condition>(2, 1.0f);

                Condition geoHashKeyCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                        .withAttributeValueList(new AttributeValue().withN(String.valueOf(geohashKey)));
                keyConditions.put(config.getGeoHashKeyColumn(), geoHashKeyCondition);

                //generate the geo hash range
                AttributeValue minRange = new AttributeValue().withN(Long.toString(range.getRangeMin()));
                AttributeValue maxRange = new AttributeValue().withN(Long.toString(range.getRangeMax()));

                Condition geoHashCondition = new Condition().withComparisonOperator(ComparisonOperator.BETWEEN)
                        .withAttributeValueList(minRange, maxRange);
                keyConditions.put(config.getGeoHashColumn(), geoHashCondition);

                queryRequest.withKeyConditions(keyConditions)
                        .withIndexName(config.getGeoIndexName());
                queryRequests.add(queryRequest);
            }
        }
        return ImmutableList.copyOf(queryRequests);
    }

    /**
     * Creates a collection of <code>GeohashRange</code> by processing each cell {@see com.google.common.geometry.S2CellId}
     * that is contained inside the given boundingBox
     *
     * @param boundingBox the boundingBox {@link com.google.common.geometry.S2LatLngRect} of a given query
     * @return ranges a list of <code>GeohashRange</code>
     */
    private List<GeohashRange> getGeoHashRanges(S2LatLngRect boundingBox) {
        S2CellUnion cells = s2Manager.findCellIds(boundingBox);
        return mergeCells(cells);
    }

    /**
     * Merge continuous cells in cellUnion and return a list of merged GeohashRanges.
     *
     * @param cellUnion Container for multiple cells.
     * @return A list of merged GeohashRanges.
     */
    private List<GeohashRange> mergeCells(S2CellUnion cellUnion) {
        List<GeohashRange> ranges = new ArrayList<GeohashRange>();
        for (S2CellId c : cellUnion.cellIds()) {
            GeohashRange range = new GeohashRange(c.rangeMin().id(), c.rangeMax().id());

            boolean wasMerged = false;
            for (GeohashRange r : ranges) {
                if (r.tryMerge(range)) {
                    wasMerged = true;
                    break;
                }
            }
            if (!wasMerged) {
                ranges.add(range);
            }
        }
        return ranges;
    }

    /**
     * Creates a copy of the provided <code>QueryRequest</code> queryRequest
     *
     * @param queryRequest
     * @return a new
     */
    private QueryRequest copyQueryRequest(QueryRequest queryRequest) {
        QueryRequest copiedQueryRequest = new QueryRequest().withAttributesToGet(queryRequest.getAttributesToGet())
                .withConsistentRead(queryRequest.getConsistentRead())
                .withExclusiveStartKey(queryRequest.getExclusiveStartKey())
                .withIndexName(queryRequest.getIndexName())
                .withKeyConditions(queryRequest.getKeyConditions())
                .withLimit(queryRequest.getLimit())
                .withReturnConsumedCapacity(queryRequest.getReturnConsumedCapacity())
                .withScanIndexForward(queryRequest.getScanIndexForward())
                .withSelect(queryRequest.getSelect())
                .withAttributesToGet(queryRequest.getAttributesToGet())
                .withTableName(queryRequest.getTableName());

        return copiedQueryRequest;
    }
}
