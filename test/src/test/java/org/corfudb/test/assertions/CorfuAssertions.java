package org.corfudb.test.assertions;

import org.assertj.core.api.Assertions;
import org.corfudb.infrastructure.CorfuServer;
import org.corfudb.infrastructure.CorfuServerAssert;

public class CorfuAssertions extends Assertions {
        public static CorfuServerAssert assertThat(CorfuServer actual) {
            return new CorfuServerAssert(actual);
        }
}
