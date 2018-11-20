package org.corfudb.recovery;

import com.google.common.collect.ImmutableList;
import org.assertj.core.data.MapEntry;
import org.corfudb.CustomSerializer;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.recovery.RecoveryFixture.CachedRecoveryFixture;
import org.corfudb.recovery.stream.StreamLogLoader.StreamLogLoaderMode;
import org.corfudb.runtime.CheckpointWriter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.collections.StringIndexer;
import org.corfudb.runtime.object.VersionLockedObject;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.ObjectBuilder;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.ObjectTypes;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Created by rmichoud on 6/16/17.
 */
public class FastObjectLoaderTest extends AbstractViewTest {
    private static final int NUMBER_OF_CHECKPOINTS = 20;
    private static final int NUMBER_OF_PUT = 100;
    private static final int SOME = 3;
    private static final int MORE = 5;


    private int key_count = 0;
    private Map<String, Map<String, String>> maps = new HashMap<>();

    private <T extends Map<String, String>> void populateMapWithNextKey(T map) {
        map.put("key" + key_count, "value" + key_count);
        key_count++;
    }

    <T extends Map<?, ?>> void populateMaps(int numMaps, CorfuRuntime rt, Class<T> type, boolean reset,
                                      int keyPerMap) {
        if (reset) {
            maps.clear();
            key_count = 0;
        }
        for (int i = 0; i < numMaps; i++) {
            String mapName = "Map" + i;
            Map<String, String> currentMap = maps.computeIfAbsent(mapName, k ->
                    Helpers.createMap(k, rt, type)
            );

            for(int j = 0; j < keyPerMap; j++) {
                populateMapWithNextKey(currentMap);
            }
        }
    }

    private void clearAllMaps() {
        maps.values().forEach(Map::clear);
    }

    private long checkPointAll(CorfuRuntime rt) throws Exception {
        MultiCheckpointWriter<Map<?, ?>> mcw = new MultiCheckpointWriter<>();
        maps.forEach((streamName, map) -> mcw.addMap(map));
        return mcw.appendCheckpoints(rt, "author").getSequence();
    }

    private void assertThatMapsAreBuilt(CorfuRuntime rt1, CorfuRuntime rt2) {
        maps.forEach((streamName, map) -> {
            Helpers.assertThatMapIsBuilt(rt1, rt2, streamName, map, CorfuTable.class);
        });
    }
    private void assertThatMapsAreBuilt(CorfuRuntime rt) {
        assertThatMapsAreBuilt(getDefaultRuntime(), rt);
    }

    private void assertThatMapsAreBuiltWhiteList(CorfuRuntime rt, List<String> whiteList) {
        maps.forEach((streamName, map) -> {
            if (whiteList.contains(streamName)) {
                Helpers.assertThatMapIsBuilt(getDefaultRuntime(), rt, streamName, map, CorfuTable.class);
            } else {
                Helpers.assertThatMapIsNotBuilt(rt, streamName, CorfuTable.class);
            }
        });
    }

    private void assertThatMapsAreBuiltIgnore(CorfuRuntime rt, String mapToIgnore) {
        maps.forEach((streamName, map) -> {
            if (!streamName.equals(mapToIgnore)) {
                Helpers.assertThatMapIsBuilt(getDefaultRuntime(), rt, streamName, map, CorfuTable.class);
            } else {
                Helpers.assertThatMapIsNotBuilt(rt, streamName, CorfuTable.class);
            }
        });
    }

    private void assertThatObjectCacheIsTheSameSize(CorfuRuntime rt1, CorfuRuntime rt2) {
        assertThat(rt2.getObjectsView().size()).isEqualTo(rt1.getObjectsView().size());
    }


    private void assertThatStreamTailsAreCorrect(Map<UUID, Long> streamTails) {
        maps.keySet().forEach((streamName) -> {
            UUID id = CorfuRuntime.getStreamID(streamName);
            long tail = getDefaultRuntime().getSequencerView().query(id).getToken().getSequence();
            if (streamTails.containsKey(id)) {
                assertThat(streamTails.get(id)).isEqualTo(tail);
            }
        });
    }

    private void assertThatStreamTailsAreCorrectIgnore(Map<UUID, Long> streamTails, String toIgnore) {
        streamTails.remove(CorfuRuntime.getStreamID(toIgnore));
        assertThatStreamTailsAreCorrect(streamTails);
    }

    /** Test that the maps are reloaded after runtime.connect()
     *
     * By checking the version of the underlying objects, we ensure that
     * they are the same in the previous runtime and the new one.
     *
     * If they are at the same version, upon access the map will not be
     * modified. All the subsequent asserts are on the pre-built map.
     *
     */
    @Test
    public void canReloadMaps() {
        populateMaps(2, getDefaultRuntime(), CorfuTable.class, true, 2);

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());
        assertThatMapsAreBuilt(rt2);
    }

    @Test
    public void canReadWithCacheDisable() {
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, true,2);

        CachedRecoveryFixture fixture = new CachedRecoveryFixture();
        fixture.getRuntimeParamsBuilder()
                .cacheDisabled(true)
                .nettyEventLoop(AbstractViewTest.NETTY_EVENT_LOOP)
                .layoutServer(getDefaultNode());
        fixture.getCustomStreamTypeBuilder().defaultObjectsType(CorfuTable.class);

        fixture.getRuntime().connect();
        fixture.getFastObjectLoader().load(fixture.getRuntime());

        assertThatMapsAreBuilt(fixture.getRuntime());
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), fixture.getRuntime());
    }


    /**
     * This test ensure that reading Holes will not affect the recovery.
     *
     */
    @Test
    public void canReadHoles() {
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, true,2);

        LogUnitClient luc = getDefaultRuntime().getLayoutView().getRuntimeLayout()
                .getLogUnitClient(getDefaultConfigurationString());
        SequencerClient seq = getDefaultRuntime().getLayoutView().getRuntimeLayout()
                .getSequencerClient(getDefaultConfigurationString());

        seq.nextToken(null, 1);
        luc.fillHole(getDefaultRuntime().getSequencerView().next().getToken());

        populateMaps(1, getDefaultRuntime(), CorfuTable.class, false, 1);

        seq.nextToken(null, 1);
        luc.fillHole(getDefaultRuntime().getSequencerView().next().getToken());

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());
        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);

    }

    /** Ensures that we are able to do rollback after the fast loading
     *
     * It is by design that reading the entries will not populate the
     * streamView context queue. Snapshot transaction to addresses before
     * the initialization log tail should seldom happen.
     *
     * In the case it does happen, the stream view will follow the backpointers
     * to figure out which are the addresses that it should rollback to.
     *
     * We don't need to optimize this path, since we don't need the stream view context
     * queue to be in a steady state. If the user really want snapshot back in time,
     * he/she will take the hit.
     *
     */
    @Test
    public void canRollBack() {
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, true, 2);
        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());

        Map<String, String> map1Prime = Helpers.createMap("Map0", rt2, CorfuTable.class);

        rt2.getObjectsView().TXBuild().setType(TransactionType.SNAPSHOT)
                .setSnapshot(new Token(0L, 0L))
                .begin();
        assertThat(map1Prime.get("key0")).isEqualTo("value0");
        assertThat(map1Prime.get("key1")).isNull();
        rt2.getObjectsView().TXEnd();

    }

    @Test
    public void canLoadWithCustomBulkRead() {
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, true, MORE);

        CachedRecoveryFixture fixture = new CachedRecoveryFixture();
        fixture.getRuntimeParamsBuilder()
                .bulkReadSize(2)
                .nettyEventLoop(AbstractViewTest.NETTY_EVENT_LOOP)
                .layoutServer(getDefaultNode());
        fixture.getCustomStreamTypeBuilder().defaultObjectsType(CorfuTable.class);

        fixture.getRuntime().connect();
        fixture.getFastObjectLoader().load(fixture.getRuntime());

        assertThatMapsAreBuilt(fixture.getRuntime());
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), fixture.getRuntime());
    }

    @Test
    public void canReadCheckpointWithoutTrim() throws Exception {
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, true, MORE);
        checkPointAll(getDefaultRuntime());

        checkPointAll(getDefaultRuntime());

        // Clear are interesting because if applied in wrong order the map might end up wrong
        clearAllMaps();
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, false, 1);

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());

        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);
    }

    @Test
    public void canReadCheckPointMultipleStream() throws Exception {
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true,1);
        checkPointAll(getDefaultRuntime());

        clearAllMaps();

        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, false, 1);

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());

        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);
    }

    @Test
    public void canReadMultipleCheckPointMultipleStreams() throws Exception {
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, 1);
        checkPointAll(getDefaultRuntime());
        clearAllMaps();

        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, false, 2);

        checkPointAll(getDefaultRuntime());
        clearAllMaps();

        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, false, 1);

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());

        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);

    }

    @Test
    public void canReadCheckPointMultipleStreamsTrim() throws Exception {
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, 1);
        long checkpointAddress = checkPointAll(getDefaultRuntime());
        clearAllMaps();

        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, false, 1);
        Helpers.trim(getDefaultRuntime(), checkpointAddress);

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());

        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);
    }

    @Test
    public void canReadCheckPointMultipleStreamTrimWithLeftOver() throws Exception {
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, 1);
        checkPointAll(getDefaultRuntime());

        // Clear are interesting because if applied in wrong order the map might end up wrong
        clearAllMaps();

        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, false, 1);

        Helpers.trim(getDefaultRuntime(), 2);

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());

        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);
    }

    @Test
    public void canFindTailsRecoverSequencerMode() {
        populateMaps(2, getDefaultRuntime(), CorfuTable.class, true, 2);

        Map<UUID, Long> streamTails = Helpers.getRecoveryStreamTails(getDefaultConfigurationString());
        assertThatStreamTailsAreCorrect(streamTails);
    }

    @Test
    public void canFindTailsWithCheckPoints() throws Exception {
        // 1 tables has 1 entry and 2 tables have 2 entries
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, 1);
        populateMaps(2, getDefaultRuntime(), CorfuTable.class, false, 1);

        int mapCount = maps.size();
        checkPointAll(getDefaultRuntime());

        Map<UUID, Long> streamTails = Helpers.getRecoveryStreamTails(getDefaultConfigurationString());
        assertThatStreamTailsAreCorrect(streamTails);
        assertThat(streamTails.size()).isEqualTo(mapCount * 2);
    }

    @Test
    public void canFindTailsWithFailedCheckpoint() {
        CorfuRuntime rt1 = getDefaultRuntime();

        Map<String, String> map = Helpers.createMap("Map1", rt1);
        map.put("k1", "v1");

        UUID stream1 = CorfuRuntime.getStreamID("Map1");

        CheckpointWriter<?> cpw = new CheckpointWriter<>(getDefaultRuntime(), stream1, "author", map);
        getDefaultRuntime().getObjectsView().TXBuild()
                .setType(TransactionType.SNAPSHOT)
                .begin();
        try {
            cpw.startCheckpoint();
            cpw.appendObjectState();
        } finally {
            getDefaultRuntime().getObjectsView().TXEnd();
        }
        map.put("k2", "v2");

        Map<UUID, Long> streamTails = Helpers.getRecoveryStreamTails(getDefaultConfigurationString());
        final int streamTailOfMap = SOME;
        assertThat(streamTails.get(stream1)).isEqualTo(streamTailOfMap);
    }

    @Test
    public void canFindTailsWithOnlyCheckpointAndTrim() throws Exception {
        // 1 tables has 1 entry and 2 tables have 2 entries
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, 1);
        populateMaps(2, getDefaultRuntime(), CorfuTable.class, false, 1);
        int mapCount = maps.size();

        long checkPointAddress = checkPointAll(getDefaultRuntime());

        Helpers.trim(getDefaultRuntime(), checkPointAddress);
        Map<UUID, Long> streamTails = Helpers.getRecoveryStreamTails(getDefaultConfigurationString());
        assertThatStreamTailsAreCorrect(streamTails);
        assertThat(streamTails.size()).isEqualTo(mapCount * 2);
    }

    @Test
    public void canReadRankOnlyEntries() throws Exception {
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, true, 2);

        LogUnitClient luc = getDefaultRuntime().getLayoutView().getRuntimeLayout()
                .getLogUnitClient(getDefaultConfigurationString());
        SequencerClient seq = getDefaultRuntime().getLayoutView().getRuntimeLayout()
                .getSequencerClient(getDefaultConfigurationString());

        long address = seq.nextToken(Collections.emptyList(),1).get().getSequence();
        ILogData data = Helpers.createEmptyData(address, DataType.RANK_ONLY,  new IMetadata.DataRank(2))
                .getSerialized();
        luc.write(data).get();

        populateMaps(1, getDefaultRuntime(), CorfuTable.class, false, 1);

        address = seq.nextToken(Collections.emptyList(),1).get().getSequence();
        data = Helpers.createEmptyData(address, DataType.RANK_ONLY,  new IMetadata.DataRank(2))
                .getSerialized();
        luc.write(data).get();

        populateMaps(1, getDefaultRuntime(), CorfuTable.class, false, 1);

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());
        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);
    }

    /**
     * Interleaving checkpoint entries and normal entries through multithreading
     *
     */
    @Test
    public void canReadWithEntriesInterleavingCPS() {
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, 1);
        int mapCount = maps.size();

        ExecutorService checkPointThread = Executors.newFixedThreadPool(1);
        checkPointThread.execute(() -> {
            for (int i = 0; i < NUMBER_OF_CHECKPOINTS; i++) {
                try {
                    checkPointAll(getDefaultRuntime());
                } catch (Exception ignored) {

                }
            }
        });


        for (int i = 0; i < NUMBER_OF_PUT; i++) {
            maps.get("Map" + (i % maps.size())).put("k" + Integer.toString(i), "v" + Integer.toString(i));
        }

        try {
            checkPointThread.shutdown();
            final int timeout = 10;
            checkPointThread.awaitTermination(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {

        }

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());
        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);


        // Also test it cans find the tails
        Map<UUID, Long> streamTails = Helpers.getRecoveryStreamTails(getDefaultConfigurationString());
        assertThatStreamTailsAreCorrect(streamTails);

        // Need to have checkpoint for each stream
        assertThat(streamTails.size()).isEqualTo(mapCount * 2);

    }

    @Test
    public void canReadWithTruncatedCheckPoint() throws Exception{
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, 1);
        long firstCheckpointAddress = checkPointAll(getDefaultRuntime());

        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, false, 1);

        checkPointAll(getDefaultRuntime());

        // Trim the log removing the checkpoint start of the first checkpoint
        Helpers.trim(getDefaultRuntime(), firstCheckpointAddress);

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());
        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);

    }

    @Test
    public void canReadLogTerminatedWithCheckpoint() throws Exception{
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, SOME);
        checkPointAll(getDefaultRuntime());

        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());

        assertThatMapsAreBuilt(rt2);
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), rt2);
    }

    @Test
    public void canReadWhenTheUserHasNoCheckpoint() {
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, SOME);

        CachedRecoveryFixture fixture = new CachedRecoveryFixture();
        fixture.getRuntimeParamsBuilder()
                .nettyEventLoop(AbstractViewTest.NETTY_EVENT_LOOP)
                .layoutServer(getDefaultNode());
        fixture.getCustomStreamTypeBuilder().defaultObjectsType(CorfuTable.class);
        fixture.getConfigBuilder().logHasNoCheckPoint(true);

        fixture.getRuntime().connect();
        fixture.getFastObjectLoader().load(fixture.getRuntime());

        assertThatMapsAreBuilt(fixture.getRuntime());
        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), fixture.getRuntime());
    }

    @Test
    public void ignoreStreamForRuntimeButNotStreamTails() {
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, SOME);

        CachedRecoveryFixture fixture = new CachedRecoveryFixture();
        fixture.getRuntimeParamsBuilder()
                .nettyEventLoop(AbstractViewTest.NETTY_EVENT_LOOP)
                .layoutServer(getDefaultNode());
        fixture.getCustomStreamTypeBuilder().defaultObjectsType(CorfuTable.class);
        fixture.getStreamLogLoaderBuilder().mode(StreamLogLoaderMode.BLACKLIST).stream("Map1");

        fixture.getRuntime().connect();
        fixture.getFastObjectLoader().load(fixture.getRuntime());

        assertThatMapsAreBuiltIgnore(fixture.getRuntime(), "Map1");

        Map<UUID, Long> streamTails = Helpers.getRecoveryStreamTails(getDefaultConfigurationString());
        assertThatStreamTailsAreCorrectIgnore(streamTails, "Map1");
    }

    @Test
    public void ignoreStreamCheckpoint() throws Exception{
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, SOME);
        checkPointAll(getDefaultRuntime());
        populateMaps(1, getDefaultRuntime(), CorfuTable.class, false, 2);

        CachedRecoveryFixture fixture = new CachedRecoveryFixture();
        fixture.getRuntimeParamsBuilder()
                .nettyEventLoop(AbstractViewTest.NETTY_EVENT_LOOP)
                .layoutServer(getDefaultNode());
        fixture.getCustomStreamTypeBuilder().defaultObjectsType(CorfuTable.class);
        fixture.getStreamLogLoaderBuilder().mode(StreamLogLoaderMode.BLACKLIST).stream("Map1");

        fixture.getRuntime().connect();
        fixture.getFastObjectLoader().load(fixture.getRuntime());

        assertThatMapsAreBuiltIgnore(fixture.getRuntime(), "Map1");
    }

    @Test
    public void doNotFailBecauseTrimIsFirst() throws Exception{
        // 1 tables has 1 entry and 2 tables have 2 entries
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, 1);
        populateMaps(2, getDefaultRuntime(), CorfuTable.class, false, 1);

        long snapShotAddress = checkPointAll(getDefaultRuntime());
        Helpers.trim(getDefaultRuntime(), snapShotAddress);

        CachedRecoveryFixture fixture = new CachedRecoveryFixture();
        fixture.getRuntimeParamsBuilder()
                .nettyEventLoop(AbstractViewTest.NETTY_EVENT_LOOP)
                .layoutServer(getDefaultNode());
        fixture.getCustomStreamTypeBuilder().defaultObjectsType(CorfuTable.class);

        fixture.getRuntime().connect();
        fixture.getFastObjectLoader().load(fixture.getRuntime());

        assertThatMapsAreBuilt(fixture.getRuntime());

        assertThatObjectCacheIsTheSameSize(getDefaultRuntime(), fixture.getRuntime());
    }

    @Test
    public void doNotReconstructTransactionStreams() {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime rt1 = getNewRuntime(getDefaultNode())
                .setTransactionLogging(true)
                .connect();
        populateMaps(SOME, rt1, CorfuTable.class, true, 2);

        rt1.getObjectsView().TXBegin();
        maps.get("Map1").put("k4", "v4");
        maps.get("Map2").put("k5", "v5");
        rt1.getObjectsView().TXEnd();

        // We need to read the maps to get to the current version in rt1
        maps.get("Map1").size();
        maps.get("Map2").size();

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = Helpers.createNewRuntimeWithFastLoader(getDefaultConfigurationString());
        assertThatMapsAreBuilt(rt1, rt2);

        // Ensure that we didn't create a map for the transaction stream.
        assertThatObjectCacheIsTheSameSize(rt1, rt2);
    }

    @Test
    public void doReconstructTransactionStreamTail() {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime rt1 = getNewRuntime(getDefaultNode())
                .setTransactionLogging(true)
                .connect();

        populateMaps(SOME, rt1, CorfuTable.class, true, 2);
        int mapCount = maps.size();

        rt1.getObjectsView().TXBegin();
        maps.get("Map1").put("k4", "v4");
        maps.get("Map2").put("k5", "v5");
        rt1.getObjectsView().TXEnd();

        // Create a new runtime with fastloader
        Map<UUID, Long> streamTails = Helpers.getRecoveryStreamTails(getDefaultConfigurationString());
        assertThatStreamTailsAreCorrect(streamTails);


        UUID transactionStreams = ObjectsView.TRANSACTION_STREAM_ID;
        long tailTransactionStream = rt1.getSequencerView().query(transactionStreams).
                getToken().getSequence();

        // Also recover the Transaction Stream
        assertThat(streamTails.size()).isEqualTo(mapCount + 1);
        assertThat(streamTails.get(transactionStreams)).isEqualTo(tailTransactionStream);


    }

    /**
     * Ensure that an empty stream (stream that was opened but never had any updates)
     * will not have its tail reconstructed. Tail of such an empty stream will be -1.
     *
     * @throws Exception exception
     */
    @Test
    public void doNotReconstructEmptyCheckpoints() throws Exception {
        // Create SOME maps and only populate 2
        populateMaps(SOME, getDefaultRuntime(), CorfuTable.class, true, 0);
        populateMaps(2, getDefaultRuntime(), CorfuTable.class, false, 2);
        CorfuRuntime rt1 = getDefaultRuntime();

        checkPointAll(rt1);
        assertThatStreamTailsAreCorrect(Helpers.getRecoveryStreamTails(getDefaultConfigurationString()));
    }

    /**
     * Upon recreation of the map, the correct serializer should be
     * set for the map. The serializer type comes from the SMREntry.
     *
     */
    @Test
    public void canRecreateMapWithCorrectSerializer() {
        ISerializer customSerializer = new CustomSerializer((byte) (Serializers.SYSTEM_SERIALIZERS_COUNT + 1));
        Serializers.registerSerializer(customSerializer);
        CorfuRuntime originalRuntime = getDefaultRuntime();

        Map<String, String> originalMap = originalRuntime
                .getObjectsView()
                .build()
                .setTypeToken(ObjectTypes.SMR_MAP_OF_STRINGS)
                .setStreamName("test")
                .setSerializer(customSerializer)
                .open();

        originalMap.put("a", "b");

        CachedRecoveryFixture fixture = new CachedRecoveryFixture();
        fixture.getRuntimeParamsBuilder()
                .nettyEventLoop(AbstractViewTest.NETTY_EVENT_LOOP)
                .layoutServer(getDefaultNode());

        fixture.getRuntime().connect();
        fixture.getFastObjectLoader().load(fixture.getRuntime());

        // We don't need to set the serializer this map
        // because it was already created in the ObjectsView cache
        // with the correct serializer.
        fixture.getRuntime()
                .getObjectsView()
                .build()
                .setTypeToken(ObjectTypes.SMR_MAP_OF_STRINGS)
                .setStreamName("test")
                .open();

        ISerializer serializer = Helpers
                .getCorfuCompileProxy(fixture.getRuntime(), "test", SMRMap.class)
                .getSerializer();

        assertThat(serializer).isEqualTo(customSerializer);
    }

    // Test will be enable after introduction of CorfuTable
    @Test
    public void canRecreateCorfuTable() {
        CorfuRuntime originalRuntime = getDefaultRuntime();

        CorfuTable<String, String> originalTable = originalRuntime
                .getObjectsView()
                .build()
                .setTypeToken(ObjectTypes.CORFU_TABLE_OF_STRINGS)
                .setStreamName("test")
                .open();

        originalTable.put("a", "b");

        CachedRecoveryFixture fixture = new CachedRecoveryFixture();

        ObjectBuilder<?> ob = new ObjectBuilder(fixture.getRuntime())
                .setType(CorfuTable.class)
                .setArguments(new StringIndexer())
                .setStreamID(CorfuRuntime.getStreamID("test"));

        Map<UUID, ObjectBuilder<?>> streamMap = new HashMap<>();
        streamMap.put(CorfuRuntime.getStreamID("test"), ob);

        fixture.getRuntimeParamsBuilder()
                .nettyEventLoop(AbstractViewTest.NETTY_EVENT_LOOP)
                .layoutServer(getDefaultNode());
        fixture.getCustomStreamTypeBuilder()
                .defaultObjectsType(CorfuTable.class)
                .customTypeStreams(streamMap);

        fixture.getRuntime().connect();
        fixture.getFastObjectLoader().load(fixture.getRuntime());

        CorfuTable<String, String> recreatedTable = fixture.getRuntime()
                .getObjectsView()
                .build()
                .setTypeToken(ObjectTypes.CORFU_TABLE_OF_STRINGS)
                .setStreamName("test")
                .open();
        // Get raw maps (VersionLockedObject)
        VersionLockedObject<?> vo1 = Helpers.getVersionLockedObject(originalRuntime, "test", CorfuTable.class);
        VersionLockedObject<?> vo1Prime = Helpers.getVersionLockedObject(fixture.getRuntime(), "test", CorfuTable.class);

        // Assert that UnderlyingObjects are at the same version
        // If they are at the same version, a sync on the object will
        // be a no op for the new runtime.
        assertThat(vo1Prime.getVersionUnsafe()).isEqualTo(vo1.getVersionUnsafe());

        assertThat(recreatedTable.get("a")).isEqualTo("b");
    }

    /**
     * Here we providing and indexer to the FastLoader. After reconstruction, we open the map
     * without specifying the indexer, but we are still able to use the indexer.
     *
     * @throws Exception exception
     */
    @Test
    public void canRecreateCorfuTableWithIndex() throws Exception {
        CorfuRuntime originalRuntime = getDefaultRuntime();

        CorfuTable<String, String> originalTable = originalRuntime
                .getObjectsView()
                .build()
                .setTypeToken(ObjectTypes.CORFU_TABLE_OF_STRINGS)
                .setArguments(new StringIndexer())
                .setStreamName("test")
                .open();

        originalTable.put("k1", "a");
        originalTable.put("k2", "ab");
        originalTable.put("k3", "ba");

        CorfuRuntime recreatedRuntime = getNewRuntime(getDefaultNode()).connect();

        MultiCheckpointWriter<Map<?, ?>> mcw = new MultiCheckpointWriter<>();
        mcw.addMap(originalTable);
        long cpAddress = mcw.appendCheckpoints(originalRuntime, "author").getSequence();
        Helpers.trim(originalRuntime, cpAddress);

        CachedRecoveryFixture fixture = new CachedRecoveryFixture();
        fixture.getRuntimeParamsBuilder()
                .nettyEventLoop(AbstractViewTest.NETTY_EVENT_LOOP)
                .layoutServer(getDefaultNode());

        fixture.getCustomTypeStreams().addIndexerToCorfuTableStream("test", new StringIndexer(), recreatedRuntime);

        fixture.getRuntime().connect();
        fixture.getFastObjectLoader().load(fixture.getRuntime());

        Helpers.assertThatMapIsBuilt(originalRuntime, recreatedRuntime, "test", originalTable, CorfuTable.class);

        // Recreating the table without explicitly providing the indexer
        CorfuTable<String, String> recreatedTable = recreatedRuntime
                .getObjectsView()
                .build()
                .setTypeToken(ObjectTypes.CORFU_TABLE_OF_STRINGS)
                .setArguments(new StringIndexer())
                .setStreamName("test")
                .open();

        assertThat(recreatedTable.getByIndex(StringIndexer.BY_FIRST_LETTER, "a"))
                .containsExactlyInAnyOrder(MapEntry.entry("k1", "a"), MapEntry.entry("k2", "ab"));

        Helpers.getVersionLockedObject(recreatedRuntime, "test", CorfuTable.class).resetUnsafe();

        recreatedTable.get("k3");
        assertThat(recreatedTable.hasSecondaryIndices()).isTrue();
        recreatedTable.getByIndex(StringIndexer.BY_FIRST_LETTER, "a");
    }

    @Test
    public void canRecreateMixOfMaps() throws Exception {
        CorfuRuntime originalRuntime = getDefaultRuntime();

        SMRMap<String, String> smrMap = originalRuntime
                .getObjectsView()
                .build()
                .setTypeToken(ObjectTypes.SMR_MAP_OF_STRINGS)
                .setStreamName("smrMap")
                .open();

        CorfuTable<String, String> corfuTable = originalRuntime
                .getObjectsView()
                .build()
                .setTypeToken(ObjectTypes.CORFU_TABLE_OF_STRINGS)
                .setStreamName("corfuTable")
                .open();

        smrMap.put("a", "b");
        corfuTable.put("c", "d");
        smrMap.put("e", "f");
        corfuTable.put("g", "h");

        MultiCheckpointWriter<Map<?, ?>> mcw = new MultiCheckpointWriter<>();
        mcw.addMap(smrMap);
        mcw.addMap(corfuTable);
        mcw.appendCheckpoints(getRuntime(), "author");

        smrMap.put("i", "j");
        corfuTable.put("k", "l");

        CachedRecoveryFixture fixture = new CachedRecoveryFixture();
        fixture.getRuntimeParamsBuilder()
                .nettyEventLoop(AbstractViewTest.NETTY_EVENT_LOOP)
                .layoutServer(getDefaultNode());

        CorfuRuntime runtime = fixture.getRuntime();
        ObjectBuilder<?> ob = new ObjectBuilder(runtime)
                .setType(CorfuTable.class)
                .setArguments(new StringIndexer())
                .setStreamID(CorfuRuntime.getStreamID("corfuTable"));

        Map<UUID, ObjectBuilder<?>> streamMap = new HashMap<>();
        streamMap.put(CorfuRuntime.getStreamID("corfuTable"), ob);

        fixture.getCustomStreamTypeBuilder().customTypeStreams(streamMap);

        System.out.println("2!!!!!!!!!!!!! " + runtime.getLayoutServers());
        runtime.connect();
        System.out.println("!!!!!!8");
        fixture.getFastObjectLoader().load(runtime);

        System.out.println("!!!!!!9");
        Helpers.assertThatMapIsBuilt(originalRuntime, runtime, "smrMap", smrMap, SMRMap.class);
        Helpers.assertThatMapIsBuilt(originalRuntime, runtime, "corfuTable", corfuTable, CorfuTable.class);
    }

    @Test
    public void canRecreateCustomWithSerializer() {
        CorfuRuntime originalRuntime = getDefaultRuntime();
        ISerializer customSerializer = new CustomSerializer((byte) (Serializers.SYSTEM_SERIALIZERS_COUNT + 1));
        Serializers.registerSerializer(customSerializer);

        CorfuTable<String, String> originalTable = originalRuntime.getObjectsView().build()
                .setTypeToken(ObjectTypes.CORFU_TABLE_OF_STRINGS)
                .setStreamName("test")
                .setSerializer(customSerializer)
                .open();

        originalTable.put("a", "b");

        CachedRecoveryFixture fixture = new CachedRecoveryFixture();

        ObjectBuilder<?> ob = new ObjectBuilder(fixture.getRuntime()).setType(CorfuTable.class)
                .setArguments(new StringIndexer())
                .setStreamID(CorfuRuntime.getStreamID("test"));

        Map<UUID, ObjectBuilder<?>> streamMap = new HashMap<>();
        streamMap.put(CorfuRuntime.getStreamID("test"), ob);

        fixture.getRuntimeParamsBuilder()
                .nettyEventLoop(AbstractViewTest.NETTY_EVENT_LOOP)
                .layoutServer(getDefaultNode());
        fixture.getCustomStreamTypeBuilder()
                .customTypeStreams(streamMap);

        fixture.getRuntime().connect();
        fixture.getFastObjectLoader().load(fixture.getRuntime());

        CorfuTable<?, ?> recreatedTable = fixture.getRuntime()
                .getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName("test")
                .open();
        // Get raw maps (VersionLockedObject)
        VersionLockedObject<?> vo1 = Helpers.getVersionLockedObject(originalRuntime, "test", CorfuTable.class);
        VersionLockedObject<?> vo1Prime = Helpers.getVersionLockedObject(
                fixture.getRuntime(), "test", CorfuTable.class
        );

        // Assert that UnderlyingObjects are at the same version
        // If they are at the same version, a sync on the object will
        // be a no op for the new runtime.
        assertThat(vo1Prime.getVersionUnsafe()).isEqualTo(vo1.getVersionUnsafe());

        assertThat(recreatedTable.get("a")).isEqualTo("b");

        ISerializer serializer = Helpers.getCorfuCompileProxy(fixture.getRuntime(), "test", CorfuTable.class)
                .getSerializer();
        assertThat(serializer).isEqualTo(customSerializer);
    }
    /**
     * FastObjectLoader can work in white list mode
     *
     * In white list mode, the only streams that will be recreated are
     * the streams that we provide and their checkpoint streams.
     */
    @Test
    public void whiteListMode() {
        populateMaps(MORE, getDefaultRuntime(), CorfuTable.class, true, SOME);

        List<String> streamsToLoad = new ArrayList<>();
        streamsToLoad.add("Map1");
        streamsToLoad.add("Map3");

        CachedRecoveryFixture fixture = new CachedRecoveryFixture();
        fixture.getRuntimeParamsBuilder()
                .nettyEventLoop(AbstractViewTest.NETTY_EVENT_LOOP)
                .layoutServer(getDefaultNode());
        fixture.getStreamLogLoaderBuilder().streams(streamsToLoad);
        fixture.getCustomStreamTypeBuilder().defaultObjectsType(CorfuTable.class);

        fixture.getRuntime().connect();
        fixture.getFastObjectLoader().load(fixture.getRuntime());

        assertThatMapsAreBuiltWhiteList(fixture.getRuntime(), streamsToLoad);
    }

    @Test
    public void whiteListModeWithCheckPoint() throws Exception {
        populateMaps(MORE, getDefaultRuntime(), CorfuTable.class, true, SOME);

        checkPointAll(getDefaultRuntime());

        // Add a transaction
        getDefaultRuntime().getObjectsView().TXBegin();
        maps.values().forEach(map-> map.put("key_tx", "val_tx"));
        getDefaultRuntime().getObjectsView().TXEnd();

        // Read all maps to get them at their current version
        maps.values().forEach(Map::size);

        ImmutableList<String> streamsToLoad = ImmutableList.of("Map1", "Map3");

        CachedRecoveryFixture fixture = new CachedRecoveryFixture();
        fixture.getRuntimeParamsBuilder()
                .nettyEventLoop(AbstractViewTest.NETTY_EVENT_LOOP)
                .layoutServer(getDefaultNode());
        fixture.getCustomStreamTypeBuilder().defaultObjectsType(CorfuTable.class);
        fixture.getStreamLogLoaderBuilder().streams(streamsToLoad);

        fixture.getRuntime().connect();
        fixture.getFastObjectLoader().load(fixture.getRuntime());

        assertThatMapsAreBuiltWhiteList(fixture.getRuntime(), streamsToLoad);
    }
}