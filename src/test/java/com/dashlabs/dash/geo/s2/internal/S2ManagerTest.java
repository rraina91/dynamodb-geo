package com.dashlabs.dash.geo.s2.internal;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


/**
 * Created by mpuri on 6/2/14
 */
public class S2ManagerTest {

    @Test
    public void testGenerateHashKey() {
        S2Manager s2Manager = new S2Manager();
        assertEquals(123, s2Manager.generateHashKey(12345678, 3));
        assertEquals(-123, s2Manager.generateHashKey(-12345678, 3));
        assertEquals(12345678, s2Manager.generateHashKey(12345678, 10));
    }
}
