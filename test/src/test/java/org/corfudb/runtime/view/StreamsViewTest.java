package org.corfudb.runtime.view;

import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.test.CorfuTest;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 2/18/16.
 */
@CorfuTest
public class StreamsViewTest {

    @CorfuTest
    public void canCopyStream(CorfuRuntime r)
            throws Exception {

        //begin tests
        UUID streamA = CorfuRuntime.getStreamID("stream A");
        UUID streamACopy = CorfuRuntime.getStreamID("stream A copy");
        byte[] testPayload = "hello world".getBytes();
        byte[] testPayloadCopy = "hello world copy".getBytes();

        IStreamView sv = r.getStreamsView().get(streamA);
        sv.append(testPayload);

        assertThat(sv.next().getPayload(r))
                .isEqualTo(testPayload);
        assertThat(sv.next())
                .isEqualTo(null);

        SequencerView sequencerView = r.getSequencerView();
        IStreamView svCopy = r.getStreamsView().copy(streamA, streamACopy,
                sequencerView.nextToken(
                        Collections.singleton(sv.getId()),
                        0).getToken().getTokenValue());

        assertThat(svCopy.next().getPayload(r))
                .isEqualTo(testPayload);
        assertThat(svCopy.next())
                .isEqualTo(null);

        svCopy.append(testPayloadCopy);

        assertThat(svCopy.next().getPayload(r))
                .isEqualTo(testPayloadCopy);
        assertThat(svCopy.next())
                .isEqualTo(null);
        assertThat(sv.next())
                .isEqualTo(null);
    }

}
