package org.corfudb.util.concurrent;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;
import org.corfudb.AbstractCorfuTest;
import org.junit.Test;

public class SingletonResourceTest extends AbstractCorfuTest {

    @Test
    public void generatedResourceIsAccessible() {
        SingletonResource<Integer> resource = SingletonResource.withInitial(() -> 0);
        assertThat(resource.get())
            .isEqualTo(0);
    }

    @Test
    public void generatedResourceIsCleanable() {
        SingletonResource<AtomicInteger> resource =
            SingletonResource.withInitial(AtomicInteger::new);
        // "Cleanup" the resource by setting it to another integer
        resource.cleanup(i -> i.set(1));
        // Access again. We should get a fresh resource
        assertThat(resource.get().get())
            .isEqualTo(0);
    }
}
