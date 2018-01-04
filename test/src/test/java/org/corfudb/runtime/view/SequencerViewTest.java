package org.corfudb.runtime.view;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.test.CorfuTest;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/23/15.
 */
@CorfuTest
public class SequencerViewTest {

    @CorfuTest
    public void canAcquireFirstToken(CorfuRuntime r) {
        assertThat(r.getSequencerView().nextToken(Collections.emptySet(), 1).getToken())
                .isEqualTo(new Token(0L, 0L));
    }

    @CorfuTest
    public void tokensAreIncrementing(CorfuRuntime r) {
        assertThat(r.getSequencerView().nextToken(Collections.emptySet(), 1).getToken())
                .isEqualTo(new Token(0L, 0L));
        assertThat(r.getSequencerView().nextToken(Collections.emptySet(), 1).getToken())
                .isEqualTo(new Token(1L, 0L));
    }

    @CorfuTest
    public void checkTokenWorks(CorfuRuntime r) {
        assertThat(r.getSequencerView().nextToken(Collections.emptySet(), 1).getToken())
                .isEqualTo(new Token(0L, 0L));
        assertThat(r.getSequencerView().nextToken(Collections.emptySet(), 0).getToken())
                .isEqualTo(new Token(0L, 0L));
    }

    @CorfuTest
    public void checkStreamTokensWork(CorfuRuntime r) {
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("stream B".getBytes());

        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 1).getToken())
                .isEqualTo(new Token(0L, 0L));
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 0).getToken())
                .isEqualTo(new Token(0L, 0L));
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamB), 1).getToken())
                .isEqualTo(new Token(1L, 0L));
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamB), 0).getToken())
                .isEqualTo(new Token(1L, 0L));
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 0).getToken())
                .isEqualTo(new Token(0L, 0L));
    }

    @CorfuTest
    public void checkBackPointersWork(CorfuRuntime r) {
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("stream B".getBytes());

        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 1).getBackpointerMap())
                .containsEntry(streamA, Address.NON_EXIST);
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 0).getBackpointerMap())
                .isEmpty();
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamB), 1).getBackpointerMap())
                .containsEntry(streamB, Address.NON_EXIST);
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamB), 0).getBackpointerMap())
                .isEmpty();
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 1).getBackpointerMap())
                .containsEntry(streamA, 0L);
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamB), 1).getBackpointerMap())
                .containsEntry(streamB, 1L);
    }
}
