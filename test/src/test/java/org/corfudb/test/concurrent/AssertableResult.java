package org.corfudb.test.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import javax.annotation.Nullable;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.AbstractThrowableAssert;

public class AssertableResult<T> {

    private final T result;
    private Throwable ex = null;

    private AssertableResult(@Nullable T result) {
        this.result = result;
    }

    public AbstractObjectAssert<?, T> assertResult()
        throws RuntimeException {
        if (ex != null) {
            throw new RuntimeException(ex);
        }
        return assertThat(result);
    }

    public AbstractThrowableAssert<?, ? extends Throwable> assertThrows() {
        if (ex == null) {
            throw new RuntimeException("Asserted an exception, but no exception was thrown!");
        }
        return assertThat(ex);
    }

    public AssertableResult<T> assertDoesNotThrowAnyExceptions() {
        assertThat(ex)
            .as("Expected thread not to throw any exceptions, but "
                + ex.getClass().getSimpleName()
                + " thrown!")
            .isNull();
        return this;
    }

    public T get() {
        return result;
    }

    public static AssertableResult<Void> voidResult() {
        return new AssertableResult<>(null);
    }

    public static <T> AssertableResult<T> of(T result) {
        return new AssertableResult<>(result);
    }
}
