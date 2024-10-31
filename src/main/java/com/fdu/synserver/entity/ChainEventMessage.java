package com.fdu.synserver.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ChainEventMessage {
    private String key;
    private String value;
    private byte operationType; // 0: insert, 1: update
    private String chainType;
    private String channelName;
    private Long updateTime;

    public byte[] toBytes() {
        return (key + "," + value + "," + operationType + "," + chainType + "," + channelName + "," + updateTime).getBytes();
    }

    public String toString() {
        return "ChainEventMessage{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", operationType=" + operationType +
                ", chainType='" + chainType + '\'' +
                ", channelName='" + channelName + '\'' +
                ", updateTime=" + updateTime +
                '}';
    }
}
