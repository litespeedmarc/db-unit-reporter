package com.scibrazeau.dbunitreporter;

import one.util.streamex.IntStreamEx;
import one.util.streamex.StreamEx;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class BQTestInterceptorIT {
    public static Stream<Arguments> provideLotsOfTestCase() {
        return IntStreamEx.range(10000)
                .mapToObj(Arguments::of);
    }

    @Test
    public void testHello() throws Throwable {
        // should invoke
        Assertions.assertEquals(1, 1);
    }

    @ParameterizedTest
    @MethodSource(value="provideLotsOfTestCase")
    public void testLargeNumberOfItems(int number) throws Throwable {
        // this doesn't really test anything.  IT is more to see logging work with large number
        // of items.
        Assertions.assertEquals(number, number);
    }

    @Test
    public void testBranchTagExtract() {
        Assertions.assertEquals("rpds-458", BQTestInterceptor.getBranchTag("feature/rpds-458"));
        Assertions.assertEquals("rpds-458", BQTestInterceptor.getBranchTag("rpds-458"));
        Assertions.assertEquals("rpds-458", BQTestInterceptor.getBranchTag("feature/rpds-458v2"));
        Assertions.assertEquals("rpds-458", BQTestInterceptor.getBranchTag("feature/rpds-458v2rpds-498/rpds-41"));
    }
}
