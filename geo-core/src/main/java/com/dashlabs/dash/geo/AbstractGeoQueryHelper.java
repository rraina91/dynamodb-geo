package com.dashlabs.dash.geo;

import com.dashlabs.dash.geo.model.GeohashRange;
import com.dashlabs.dash.geo.s2.internal.S2Manager;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2CellUnion;
import com.google.common.geometry.S2LatLngRect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * User: blangel
 * Date: 8/1/17
 * Time: 9:06 AM
 */
public abstract class AbstractGeoQueryHelper {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractGeoQueryHelper.class.getSimpleName());

    protected final S2Manager s2Manager;

    protected AbstractGeoQueryHelper(S2Manager s2Manager) {
        this.s2Manager = s2Manager;
    }

    /**
     * Creates a collection of <code>GeohashRange</code> by processing each cell {@see com.google.common.geometry.S2CellId}
     * that is contained inside the given boundingBox
     *
     * @param boundingBox the boundingBox {@link com.google.common.geometry.S2LatLngRect} of a given query
     * @return ranges a list of <code>GeohashRange</code>
     */
    protected List<GeohashRange> getGeoHashRanges(S2LatLngRect boundingBox) {
        S2CellUnion cells = s2Manager.findCellIds(boundingBox);
        return mergeCells(cells);
    }

    /**
     * Merge continuous cells in cellUnion and return a list of merged GeohashRanges.
     *
     * @param cellUnion Container for multiple cells.
     * @return A list of merged GeohashRanges.
     */
    protected List<GeohashRange> mergeCells(S2CellUnion cellUnion) {
        List<S2CellId> cellIds = cellUnion.cellIds();
        if (cellIds.size() > 1000) {
            LOG.warn("Created [{}] cell ids", cellIds.size());
        }
        List<GeohashRange> ranges = new ArrayList<>(cellIds.size());
        for (S2CellId c : cellIds) {
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

}
