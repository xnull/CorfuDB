package org.corfudb.test.assertions;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.StubMethod;
import net.bytebuddy.implementation.SuperMethodCall;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.matcher.ElementMatchers;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.corfudb.infrastructure.CorfuServer;
import org.corfudb.infrastructure.CorfuServerAssert;
import org.junit.runners.model.MultipleFailureException;

public class CorfuAssertions extends Assertions {

        public static CorfuServerAssert assertThat(CorfuServer actual) {
            return new CorfuServerAssert(actual);
        }


        public static @Nonnull
        <A extends AbstractAssert<A, T>, T> A
        assertThatQuorumOf(Class<A> assertionType, T... object) {
            return assertThatQuorumOf(assertionType, object);
        }

        public static @Nonnull
        CorfuServerAssert assertThatQuorumOf(CorfuServer... object) {
            return assertThatQuorumOf(CorfuServerAssert.class, object);
        }
}
