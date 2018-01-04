package org.corfudb.runtime.object.transactions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.test.CorfuTest;
import org.corfudb.test.concurrent.AssertableResult;
import org.corfudb.test.concurrent.CorfuTestThread;
import org.junit.Test;

public interface ITransactionTest<T extends AbstractTransactionalContext> {

    void txBegin(CorfuRuntime runtime);

    default long txEnd(CorfuRuntime runtime) {
        return runtime.getObjectsView().TXEnd();
    }

    default void txAbort(CorfuRuntime runtime) {
        runtime.getObjectsView().TXAbort();
    }

    public class TransactionThread<K, V, T extends ITransactionTest>
        extends CorfuTestThread<T, SMRMap<K, V>> {

        V put(@Nonnull K key, V value) {
            return getObject().put(key, value);
        }

        void write(@Nonnull K key, V value) {
            getObject().blindPut(key, value);
        }

        V get(@Nonnull K key) {
            return getObject().get(key);
        }

        void forEach(@Nonnull BiConsumer<? super K, ? super V> action) {
            getObject().forEach(action);
        }

        void txBegin() {
            getTest().txBegin(getRuntime());
        }

        long txEnd() {
            return getTest().txEnd(getRuntime());
        }

        void txAbort() {
            getTest().txAbort(getRuntime());
        }
    }

    /** Ensure that empty write sets are not written to the log.
     * This test applies to all contexts which is why it is in
     * the abstract test.
     */
    @CorfuTest
    default void ensureEmptyWriteSetIsNotWritten(CorfuRuntime runtime) {
        txBegin(runtime);
        long result = txEnd(runtime);

        ILogData ld =
            runtime
                .getAddressSpaceView()
                .peek(0);

        assertThat(ld)
            .isNull();

        assertThat(result)
            .isEqualTo(AbstractTransactionalContext.NOWRITE_ADDRESS);
    }

}
