package com.fdu.synserver.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SignatureMessage {
    private String key;
    private String dbName;
    private String tableName;
    private String signature;
}
