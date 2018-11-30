package org.corfudb.universe.dynamic;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@ToString
public class ContainerStats {

    public float cpuUsage;

    public ContainerStats updateCpuUsage(float cpuUsage) {
        this.cpuUsage = cpuUsage;
        return this;
    }
}
