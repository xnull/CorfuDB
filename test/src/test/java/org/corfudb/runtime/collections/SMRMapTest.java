package org.corfudb.runtime.collections;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.test.CorfuTest;
import org.corfudb.test.parameters.CorfuObjectParameter;

@CorfuTest
public class SMRMapTest implements IMapTest<SMRMap> {

    @CorfuTest
    public void canWriteScanAndFilterToSingle(CorfuRuntime runtime,
        @CorfuObjectParameter SMRMap<String, String> corfuInstancesMap) {
        corfuInstancesMap.clear();
        assertThat(corfuInstancesMap.put("a", "CorfuServer"))
            .isNull();
        assertThat(corfuInstancesMap.put("b", "CorfuClient"))
            .isNull();
        assertThat(corfuInstancesMap.put("c", "CorfuClient"))
            .isNull();
        assertThat(corfuInstancesMap.put("d", "CorfuServer"))
            .isNull();

        // ScanAndFilterByEntry
        Predicate<Entry<String, String>> valuePredicate =
            p -> p.getValue().equals("CorfuServer");
        Collection<Entry<String, String>> filteredMap = corfuInstancesMap
            .scanAndFilterByEntry(valuePredicate);

        assertThat(filteredMap.size()).isEqualTo(2);

        for(Map.Entry<String, String> corfuInstance : filteredMap) {
            assertThat(corfuInstance.getValue()).isEqualTo("CorfuServer");
        }

        // ScanAndFilter (Deprecated Method)
        List<String> corfuServerList = corfuInstancesMap
            .scanAndFilter(p -> p.equals("CorfuServer"));

        assertThat(corfuServerList.size()).isEqualTo(2);

        for(String corfuInstance : corfuServerList) {
            assertThat(corfuInstance).isEqualTo("CorfuServer");
        }
    }
}
