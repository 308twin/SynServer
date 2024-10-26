package com.fdu.synserver.service;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.shaded.com.google.protobuf.Timestamp;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.springframework.stereotype.Service;

@Service
public class MQService {

    private static final Log LOG = LogFactory.getLog(MQService.class);


    @org.springframework.beans.factory.annotation.Value("${my.custom.config.isServer}")
    private boolean isServer;

    @org.springframework.beans.factory.annotation.Value("${spring.rocketmq.proxy.server-address}")
    private String proxyServerAddress;

    @org.springframework.beans.factory.annotation.Value("${spring.rocketmq.topic.syn}")
    private String synTopic;

    @org.springframework.beans.factory.annotation.Value("${spring.rocketmq.topic.syn}")
    private String signatureTopic;

    private ClientServiceProvider provider;
    private ClientConfiguration clientConfiguration;
    
    private final DBService dbService;
    private final SignatureService signatureService;
    
    public MQService(DBService dbService, SignatureService signatureService) {
        this.dbService = dbService;
        this.signatureService = signatureService;
    }

    public void initConsumer() throws ClientException{
        provider = ClientServiceProvider.loadService();
            clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(proxyServerAddress)
            .build();
    }

    public void processMessage(){

    }


}
