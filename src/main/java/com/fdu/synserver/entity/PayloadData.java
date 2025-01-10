package com.fdu.synserver.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PayloadData {

    private Payload payload;
    private Sender sender;
    private Result result;

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Payload {
        private String chainId;
        private String txId;
        private long timestamp;
        private String contractName;
        private String method;
        private List<Parameter> parameters;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Parameter {
        private String key;
        private String value;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Sender {
        private Signer signer;
        private String signature;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Signer {
        private String orgId;
        private String memberInfo;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Result {
        private ContractResult contractResult;
        private String rwSetHash;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ContractResult {
        private String result;
        private String message;
    }
}
