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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.fdu.synserver.entity.ChainEventMessage;
import com.fdu.synserver.entity.SignatureMessage;

import jakarta.annotation.PostConstruct;

//与MQClient的交互逻辑，只有Client端与MQClient交互。
@Service
public class MQClientService {

    private static final Log LOG = LogFactory.getLog(MQClientService.class);

    @org.springframework.beans.factory.annotation.Value("${my.custom.config.isServer}")
    private boolean isServer;

    @org.springframework.beans.factory.annotation.Value("${spring.rocketmqLocal.proxy.server-address}")
    private String proxyServerAddress;

    @org.springframework.beans.factory.annotation.Value("${spring.rocketmqLocal.topic.syn}")
    private String synTopic;

    @org.springframework.beans.factory.annotation.Value("${spring.rocketmqLocal.topic.signature}")
    private String signatureTopic;

    @org.springframework.beans.factory.annotation.Value("${spring.datasource.dbName}")
    private String dbName;

    
    private ClientServiceProvider provider;
    private ClientConfiguration clientConfiguration;
    private PushConsumer pushConsumer;
    private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(Kryo::new);

    private final DBService dbService;
    private final SignatureService signatureService;
    private final MQServerService mqServerService;

     
    public MQClientService(DBService dbService, SignatureService signatureService, MQServerService mqServerService) {
        this.dbService = dbService;
        this.signatureService = signatureService;
        this.mqServerService = mqServerService;
    }

    @PostConstruct
    public void initConsumer() throws ClientException {
        if(isServer) {
            return;
        }
        provider = ClientServiceProvider.loadService();
        clientConfiguration = ClientConfiguration.newBuilder()
                .setEndpoints(proxyServerAddress)
                .build();
        String tag = "*";
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        pushConsumer = provider.newPushConsumerBuilder()
                    .setClientConfiguration(clientConfiguration)
                    .setConsumerGroup("record_consumer") // 设置 Consumer Group
                    .setSubscriptionExpressions(Collections.singletonMap(synTopic, filterExpression))
                    .setMessageListener(messageView -> {
                        // LOG.info("Consume message successfully, messageId="+
                        // messageView.getMessageId());
                        processMessage(messageView);
                        return ConsumeResult.SUCCESS;
                    })
                    .build();
    }

    public void processMessage(MessageView messageView) {
        LOG.debug("Received message: " + messageView.getMessageId());
        String tag = messageView.getTag().orElse(null); // tag就是数据库表的名字，具体为chainType+channelName
        ByteBuffer body = messageView.getBody();
        Kryo kryo = kryoThreadLocal.get();
        byte[] byteArray = new byte[body.remaining()];
        // 使用 body 的只读缓冲区创建一个副本，并将其内容读入 byteArray
        body.duplicate().get(byteArray);

        // 将字节数组包装成 Input 对象
        Input input = new Input(byteArray);
        ChainEventMessage chainEventMessage;
        try {
            chainEventMessage = kryo.readObject(input, ChainEventMessage.class);
        } catch (Exception e) {
            LOG.error("Failed to deserialize message: " + messageView.getMessageId());
            input.close();
            return;
        }

        // 构造SQL并将语句加入待执行队列
        String sql =  dbService.buildSQL(chainEventMessage);
        dbService.addSQL(sql);

        // 计算签名并发送
        String tableName = chainEventMessage.getChainType() + "_" + chainEventMessage.getChannelName();
        String signature = signatureService.signData(tableName, chainEventMessage.toBytes());
        SignatureMessage signatureMessage = new SignatureMessage(chainEventMessage.getKey(), dbName,tableName,signature);
        mqServerService.sendSignature(signatureMessage);
    }


}
