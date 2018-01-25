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
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.matcher.ElementMatchers;
import org.assertj.core.api.AbstractAssert;

public class QuorumAssert {

    public static class QuorumAssertionError extends AssertionError {

        final List<Throwable> assertionErrors;
        final int total;

        public QuorumAssertionError(int total, List<Throwable> errors) {
            this.total = total;
            this.assertionErrors = errors;
        }


        @Override
        public String getMessage() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("Expected a quorum of %d/%d "
                    + "but got %d failures\n",
                total/2 + 1, total, assertionErrors.size()));
            sb.append("Failures were:");
            for (Throwable error : assertionErrors) {
                sb.append(String.format("\n%s: %s", error.getClass().getSimpleName(),
                    error.getMessage()));
            }
            return sb.toString();
        }
    }

    public static class QuorumAsserter<A extends AbstractAssert<A, T>, T> {

        final T[] objects;
        final Class<A> assertionType;

        public QuorumAsserter(Class<A> assertionType, T[] objects) {
            this.objects = objects;
            this.assertionType = assertionType;
        }

        @RuntimeType
        @SuppressWarnings("unchecked")
        public Object invoke(
            @Origin Method method,
            @AllArguments Object[] arguments) throws Exception {
            Optional<Constructor<A>> asserterCtor
                = Arrays.stream(assertionType.getDeclaredConstructors())
                .filter(ctor -> ctor.getParameterCount() == 1)
                .map(ctor -> (Constructor<A>) ctor)
                .findFirst();
            if (!asserterCtor.isPresent()) {
                throw new UnsupportedOperationException();
            }
            List<Throwable> assertionExceptions = new ArrayList<>();
            for (T object : objects) {
                try {
                    A asserter = asserterCtor.get().newInstance(object);
                    method.invoke(asserter, arguments);
                } catch (InvocationTargetException e) {
                    assertionExceptions.add(e.getCause());
                }
            }
            if (assertionExceptions.size() > (objects.length / 2)) {
                throw new QuorumAssertionError(objects.length, assertionExceptions);
            }
            return this;
        }
    }

    public static @Nonnull
    <A extends AbstractAssert<A, T>, T> A
    assertThatQuorumOf(Class<A> assertionType, T... object) {
        Constructor constructor = assertionType.getDeclaredConstructors()[0];
        Object[] emptyArgs = new Object[constructor.getParameterCount()];
        Class<? extends A> augmentedClass = new ByteBuddy()
            .subclass(assertionType)
            .method(ElementMatchers.not(ElementMatchers.isDeclaredBy(Object.class)))
            .intercept(MethodDelegation.to(new QuorumAsserter<A, T>(assertionType, object)))
            .defineConstructor(Visibility.PUBLIC)
            .intercept(MethodCall
                .invoke(constructor)
                .with(emptyArgs))
            .make()
            .load(assertionType.getClassLoader())
            .getLoaded();
        try {
            return augmentedClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
