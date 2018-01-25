package org.corfudb.test.assertions;

import javax.annotation.Nonnull;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.corfudb.infrastructure.CorfuServer;
import org.corfudb.infrastructure.CorfuServerAssert;

public class CorfuAssertions extends Assertions {

        public static CorfuServerAssert assertThat(CorfuServer actual) {
            return new CorfuServerAssert(actual);
        }


        public static @Nonnull
        <A extends AbstractAssert<A, T>, T> A
        assertThatQuorumOf(Class<A> assertionType, T... object) {
            return QuorumAssert.assertThatQuorumOf(assertionType, object);
        }

        public static @Nonnull
        CorfuServerAssert assertThatQuorumOf(CorfuServer... object) {
            return assertThatQuorumOf(CorfuServerAssert.class, object);
        }
}
