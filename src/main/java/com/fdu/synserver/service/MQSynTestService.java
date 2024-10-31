package com.fdu.synserver.service;

import org.springframework.stereotype.Service;
import java.nio.ByteBuffer;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.tomcat.util.http.fileupload.ByteArrayOutputStream;
import org.springframework.stereotype.Service;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fdu.synserver.entity.ChainEventMessage;

import jakarta.annotation.PostConstruct;
@Service
public class MQSynTestService { //测试用，向本地MQ发送消息
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

    private Producer synProducer;
    private ClientServiceProvider provider;
    private ClientConfiguration clientConfiguration;
    private PushConsumer pushConsumer;
    private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(Kryo::new);

    private final DBService dbService;
    private final SignatureService signatureService;
    private final MQServerService mqServerService;

    public MQSynTestService(DBService dbService, SignatureService signatureService, MQServerService mqServerService) {
        this.dbService = dbService;
        this.signatureService = signatureService;
        this.mqServerService = mqServerService;
    }


    @PostConstruct
    public void initSynProducer() throws ClientException {
       
        provider = ClientServiceProvider.loadService();
        clientConfiguration = ClientConfiguration.newBuilder()
                .setEndpoints(proxyServerAddress)
                .build();
        ClientConfigurationBuilder builder = ClientConfiguration.newBuilder().setEndpoints(proxyServerAddress);
        ClientConfiguration configuration = builder.build();
        synProducer = provider.newProducerBuilder()
                .setClientConfiguration(configuration)
                .build();
        LOG.info("Test Producer init success");
    }

    //通过syntopic发送区块链事件消息
    public void sendChainEventMessage(ChainEventMessage chainEventMessage) {
        Kryo kryo = kryoThreadLocal.get();
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream(); // 重用字节输出流
        Output output = new Output(byteOut); // 重用 Kryo 的 Output 对象
        kryo.writeObject(output, chainEventMessage);
        output.flush();

        byte[] serializedBytes = byteOut.toByteArray(); // 获取序列化后的字节数组

        Message message = provider.newMessageBuilder()
                .setTopic(synTopic)
                .setTag(chainEventMessage.getChainType()+"_"+chainEventMessage.getChannelName())
                .setBody(serializedBytes)
                .build();
        try {
            LOG.info("Try to send message");
            // 发送消息，需要关注发送结果，并捕获失败等异常。
            SendReceipt sendReceipt = synProducer.send(message);
            LOG.info("Send message successfully, messageId=" + sendReceipt.getMessageId()
                    + " topic = " + signatureTopic
                    + " tag=" + chainEventMessage.getChainType()+"_"+chainEventMessage.getChannelName());

        } catch (ClientException e) {
            LOG.error("Failed to send message", e);
        }
    }
}
