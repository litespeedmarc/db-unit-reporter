package com.scibrazeau.dbunitreporter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BQTestInterceptorIT {
    @Test
    public void testHello() throws Throwable {
        // should invoke
        Assertions.assertEquals(1, 1);
    }

    @Test
    public void testBranchTagExtract() {
        Assertions.assertEquals("rpds-458", BQTestInterceptor.getBranchTag("feature/rpds-458"));
        Assertions.assertEquals("rpds-458", BQTestInterceptor.getBranchTag("rpds-458"));
        Assertions.assertEquals("rpds-458", BQTestInterceptor.getBranchTag("feature/rpds-458v2"));
        Assertions.assertEquals("rpds-458", BQTestInterceptor.getBranchTag("feature/rpds-458v2rpds-498/rpds-41"));
    }
}
