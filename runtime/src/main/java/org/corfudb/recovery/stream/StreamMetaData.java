package org.corfudb.recovery.stream;

import lombok.Data;
import lombok.experimental.Accessors;
import org.corfudb.runtime.view.Address;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Data
public class StreamMetaData {
    private final UUID streamId;
    private CheckPoint latestCheckPoint;
    private final Map<UUID, CheckPoint> checkPoints = new HashMap<>();

    public Long getHeadAddress() {
        return latestCheckPoint != null ? latestCheckPoint.snapshotAddress : Address.NEVER_READ;
    }

    public void addCheckPoint(CheckPoint cp) {
        checkPoints.put(cp.getCheckPointId(), cp);
    }

    public CheckPoint getCheckPoint(UUID checkPointId) {
        return checkPoints.get(checkPointId);
    }

    public boolean checkPointExists(UUID checkPointId) {
        return checkPoints.containsKey(checkPointId);
    }

    public void updateLatestCheckpointIfLater(UUID checkPointId) {
        CheckPoint contender = getCheckPoint(checkPointId);
        if (latestCheckPoint == null || contender.getSnapshotAddress() > latestCheckPoint.getSnapshotAddress()) {
            latestCheckPoint = contender;
        }
    }

    @Data
    @Accessors(chain = true)
    public static class CheckPoint {
        final UUID checkPointId;
        long snapshotAddress;
        long startAddress;
        boolean ended = false;
        boolean started = false;
        List<Long> addresses = new ArrayList<>();

        public CheckPoint addAddress(long address) {
            addresses.add(address);
            return this;
        }
    }
}
