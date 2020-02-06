package com.atmasapps.amazonaws.geo;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

import com.atmasapps.dashlabs.dash.geo.AbstractGeoQueryHelper;
import com.atmasapps.dashlabs.dash.geo.model.GeohashRange;
import com.atmasapps.dashlabs.dash.geo.s2.internal.S2Manager;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.geometry.S2LatLngRect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mpuri on 3/25/14
 * 
 * Corkhounds.com altered this class to add copy over the FilterExpression and 
 * Expression Attributes as necessary when building a QueryRequest. 
 */
public class GeoQueryHelper extends AbstractGeoQueryHelper {

    public GeoQueryHelper(S2Manager s2Manager) {
        super(s2Manager);
    }

    /**
     * For the given <code>QueryRequest</code> query and the boundingBox, this method creates a collection of queries
     * that are decorated with geo attributes to enable geo-spatial querying.
     *
     * @param query       the original query request
     * @param boundingBox the bounding lat long rectangle of the geo query
     * @param config      the config containing caller's geo config, example index name, etc.
     * @param compositeKeyValue the value of the column that is used in the construction of the composite hash key(geoHashKey + someOtherColumnValue).
     *                          This is needed when constructing queries that need a composite hash key.
     *                          For eg. Fetch an item where lat/long is 23.78787, -70.6767 AND category = 'restaurants'
     * @return queryRequests an immutable collection of <code>QueryRequest</code> that are now "geo enabled"
     */
    public List<QueryRequest> generateGeoQueries(QueryRequest query, S2LatLngRect boundingBox, GeoConfig config, Optional<String> compositeKeyValue) {
        List<GeohashRange> outerRanges = getGeoHashRanges(boundingBox);
        List<QueryRequest> queryRequests = new ArrayList<QueryRequest>(outerRanges.size());
        //Create multiple queries based on the geo ranges derived from the bounding box
        for (GeohashRange outerRange : outerRanges) {
            List<GeohashRange> geohashRanges = outerRange.trySplit(config.getGeoHashKeyLength(), s2Manager);
            for (GeohashRange range : geohashRanges) {
                //Make a copy of the query request to retain original query attributes like table name, etc.
                QueryRequest.Builder queryRequest = copyQueryRequest(query).toBuilder();

                //generate the hash key for the global secondary index
                long geohashKey = s2Manager.generateHashKey(range.getRangeMin(), config.getGeoHashKeyLength());
                Map<String, Condition> keyConditions = new HashMap<String, Condition>(2, 1.0f);

                //Construct the hashKey condition
                Condition geoHashKeyCondition;
                if (config.getHashKeyDecorator().isPresent() && compositeKeyValue.isPresent()) {
                    String compositeHashKey = config.getHashKeyDecorator().get().decorate(compositeKeyValue.get(), geohashKey);
                    geoHashKeyCondition = Condition.builder().comparisonOperator(ComparisonOperator.EQ)
                            .attributeValueList(AttributeValue.builder().s(compositeHashKey).build()).build();
                } else {
                    geoHashKeyCondition = Condition.builder().comparisonOperator(ComparisonOperator.EQ)
                            .attributeValueList(AttributeValue.builder().n(String.valueOf(geohashKey)).build()).build();
                }
                keyConditions.put(config.getGeoHashKeyColumn(), geoHashKeyCondition);

                //generate the geo hash range
                AttributeValue minRange = AttributeValue.builder().n(Long.toString(range.getRangeMin())).build();
                AttributeValue maxRange = AttributeValue.builder().n(Long.toString(range.getRangeMax())).build();

                Condition geoHashCondition = Condition.builder().comparisonOperator(ComparisonOperator.BETWEEN)
                        .attributeValueList(minRange, maxRange).build();
                keyConditions.put(config.getGeoHashColumn(), geoHashCondition);

                queryRequest.keyConditions(keyConditions)
                        .indexName(config.getGeoIndexName());
                queryRequests.add(queryRequest.build());
            }
        }
        return ImmutableList.copyOf(queryRequests);
    }

    /**
     * Creates a copy of the provided <code>QueryRequest</code> queryRequest
     *
     * @param queryRequest
     * @return a new
     */
    private QueryRequest copyQueryRequest(QueryRequest queryRequest) {
        QueryRequest copiedQueryRequest = QueryRequest.builder().attributesToGet(queryRequest.attributesToGet())
                .consistentRead(queryRequest.consistentRead())
                .exclusiveStartKey(queryRequest.exclusiveStartKey())
                .indexName(queryRequest.indexName())
                .keyConditions(queryRequest.keyConditions())
                .limit(queryRequest.limit())
                .returnConsumedCapacity(queryRequest.returnConsumedCapacity())
                .scanIndexForward(queryRequest.scanIndexForward())
                .select(queryRequest.select())
                .attributesToGet(queryRequest.attributesToGet())
                .tableName(queryRequest.tableName())
                .filterExpression(queryRequest.filterExpression())
                .expressionAttributeNames(queryRequest.expressionAttributeNames())
                .expressionAttributeValues(queryRequest.expressionAttributeValues()).build();

        return copiedQueryRequest;
    }
}
