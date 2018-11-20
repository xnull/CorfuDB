package org.corfudb.util;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.SMRMap;

public class ObjectTypes {

    private ObjectTypes() {
        //prevent creating instances of ObjectTypes
    }

    public static final TypeToken<CorfuTable<String, String>> CORFU_TABLE_OF_STRINGS = corfuTableType();
    public static final TypeToken<SMRMap<String, String>> SMR_MAP_OF_STRINGS = smrMapType();

    public static <K, V> TypeToken<CorfuTable<K, V>> corfuTableType() {
        return new TypeToken<CorfuTable<K, V>>() {
        };
    }

    public static <K, V> TypeToken<SMRMap<K, V>> smrMapType() {
        return new TypeToken<SMRMap<K, V>>() {
        };
    }
}
