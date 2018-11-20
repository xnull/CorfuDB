package org.corfudb.util;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class ClassCastUtil {

    private ClassCastUtil() {
        //prevent creating class util instances
    }

    @SuppressWarnings("unchecked")
    public static <T> Optional<T> cast(Object obj) {
        return Optional
                .ofNullable(obj)
                .flatMap(o -> {
                    try {
                        return Optional.of((T) o);
                    } catch (ClassCastException e) {
                        log.error("Class cast exception", e);
                    }
                    return Optional.empty();
                });
    }

    public static <T> Optional<T> cast(Object obj, @NonNull Class<T> objType) {

        Optional<T> result = Optional
                .ofNullable(obj)
                .filter(o -> !objType.isInstance(obj))
                .map(objType::cast);

        if (!result.isPresent()){
            log.error("Can't cast object to type: {}", objType, new IllegalArgumentException());
        }

        return result;
    }
}
