package com.dashlabs.dash.geo.s2.internal;

import com.google.common.geometry.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * User: blangel
 * Date: 7/19/17
 * Time: 1:34 PM
 *
 * Originally from {@code com.amazonaws.geo.s2.internal.S2Manager} in DynamoDB-Geo (https://github.com/awslabs/dynamodb-geo),
 * moved to share across dynamodb-geo and s3 equivalent.
 */
public class S2Manager {

    public S2CellUnion findCellIds(S2LatLngRect latLngRect) {

        ConcurrentLinkedQueue<S2CellId> queue = new ConcurrentLinkedQueue<S2CellId>();
        ArrayList<S2CellId> cellIds = new ArrayList<S2CellId>();

        for (S2CellId c = S2CellId.begin(0); !c.equals(S2CellId.end(0)); c = c.next()) {
            if (containsGeodataToFind(c, latLngRect)) {
                queue.add(c);
            }
        }

        processQueue(queue, cellIds, latLngRect);
        assert queue.size() == 0;
        queue = null;

        if (cellIds.size() > 0) {
            S2CellUnion cellUnion = new S2CellUnion();
            cellUnion.initFromCellIds(cellIds); // This normalize the cells.
            // cellUnion.initRawCellIds(cellIds); // This does not normalize the cells.
            cellIds = null;

            return cellUnion;
        }

        return null;
    }

    private boolean containsGeodataToFind(S2CellId c, S2LatLngRect latLngRect) {
        if (latLngRect != null) {
            return latLngRect.intersects(new S2Cell(c));
        }

        return false;
    }

    private void processQueue(ConcurrentLinkedQueue<S2CellId> queue, ArrayList<S2CellId> cellIds,
                              S2LatLngRect latLngRect) {
        for (S2CellId c = queue.poll(); c != null; c = queue.poll()) {

            if (!c.isValid()) {
                break;
            }

            processChildren(c, latLngRect, queue, cellIds);
        }
    }

    private void processChildren(S2CellId parent, S2LatLngRect latLngRect,
                                 ConcurrentLinkedQueue<S2CellId> queue, ArrayList<S2CellId> cellIds) {
        List<S2CellId> children = new ArrayList<S2CellId>(4);

        for (S2CellId c = parent.childBegin(); !c.equals(parent.childEnd()); c = c.next()) {
            if (containsGeodataToFind(c, latLngRect)) {
                children.add(c);
            }
        }

		/*
		 * TODO: Need to update the strategy!
		 *
		 * Current strategy:
		 * 1 or 2 cells contain cellIdToFind: Traverse the children of the cell.
		 * 3 cells contain cellIdToFind: Add 3 cells for result.
		 * 4 cells contain cellIdToFind: Add the parent for result.
		 *
		 * ** All non-leaf cells contain 4 child cells.
		 */
        if (children.size() == 1 || children.size() == 2) {
            for (S2CellId child : children) {
                if (child.isLeaf()) {
                    cellIds.add(child);
                } else {
                    queue.add(child);
                }
            }
        } else if (children.size() == 3) {
            cellIds.addAll(children);
        } else if (children.size() == 4) {
            cellIds.add(parent);
        } else {
            assert false; // This should not happen.
        }
    }

    public long generateGeohash(double latitude, double longitude) {
        S2LatLng latLng = S2LatLng.fromDegrees(latitude, longitude);
        S2Cell cell = new S2Cell(latLng);
        S2CellId cellId = cell.id();

        return cellId.id();
    }

    public long generateHashKey(long geohash, int hashKeyLength) {
        if (geohash < 0) {
            // Counteract "-" at beginning of geohash.
            hashKeyLength++;
        }

        String geohashString = String.valueOf(geohash);
        long denominator = (long) Math.pow(10, geohashString.length() - hashKeyLength);
        if (denominator ==
                0) { //  can happen if geohashString.length() < geohash. Querying with a lat/lng of 0.0 can create this situation.
            return geohash;
        }
        return geohash / denominator;
    }

    /**
     * Creates a bounding box for a radius query
     *
     * @param latitude  the latitude of the radius center
     * @param longitude the longitude of the radius center
     * @param radius    the radius
     * @return the bounding box
     */
    public S2LatLngRect getBoundingBoxForRadiusQuery(double latitude, double longitude, double radius) {
        S2LatLng centerLatLng = S2LatLng.fromDegrees(latitude, longitude);
        double latReferenceUnit = latitude > 0.0 ? -1.0 : 1.0;
        S2LatLng latReferenceLatLng = S2LatLng.fromDegrees(latitude + latReferenceUnit,
                longitude);
        double lngReferenceUnit = longitude > 0.0 ? -1.0 : 1.0;
        S2LatLng lngReferenceLatLng = S2LatLng.fromDegrees(latitude, longitude
                + lngReferenceUnit);

        double latForRadius = radius / centerLatLng.getEarthDistance(latReferenceLatLng);
        double lngForRadius = radius / centerLatLng.getEarthDistance(lngReferenceLatLng);

        S2LatLng minLatLng = S2LatLng.fromDegrees(latitude - latForRadius,
                longitude - lngForRadius);
        S2LatLng maxLatLng = S2LatLng.fromDegrees(latitude + latForRadius,
                longitude + lngForRadius);

        return new S2LatLngRect(minLatLng, maxLatLng);
    }

    /**
     * Creates a bounding box for a rectangle query
     *
     * @param minLatitude  the min latitude of the rectangle
     * @param minLongitude the min longitude of the rectangle
     * @param maxLatitude  the max latitude of the rectangle
     * @param maxLongitude the max longitude of the rectangle
     * @return the bounding box
     */
    public S2LatLngRect getBoundingBoxForRectangleQuery(double minLatitude, double minLongitude, double maxLatitude, double maxLongitude) {
        S2LatLng minLatLng = S2LatLng.fromDegrees(minLatitude, minLongitude);
        S2LatLng maxLatLng = S2LatLng.fromDegrees(maxLatitude, maxLongitude);
        return new S2LatLngRect(minLatLng, maxLatLng);
    }

}
