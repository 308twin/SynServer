package com.fdu.synserver.entity;

import java.nio.charset.StandardCharsets;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
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
        return (key + "," + value + "," + operationType + "," + chainType + "," + channelName + "," + updateTime).getBytes(StandardCharsets.UTF_8);
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
