package org.corfudb.infrastructure;

import org.assertj.core.api.AbstractAssert;

public class CorfuServerAssert extends AbstractAssert<CorfuServerAssert, CorfuServer> {

    public CorfuServerAssert(CorfuServer actual) {
        super (actual, CorfuServerAssert.class);
    }

    public static CorfuServerAssert assertThat(CorfuServer actual) {
        return new CorfuServerAssert(actual);
    }

    public CorfuServerAssert hasEpoch(long expected) {
        isNotNull();

        final long serverEpoch = actual.getServerContext().getServerEpoch();

        if (serverEpoch != expected) {
            failWithMessage(
                "Expected server to be in epoch <%d> but it was in <%d>",
                                expected, serverEpoch);
        }

        return this;
    }
}
