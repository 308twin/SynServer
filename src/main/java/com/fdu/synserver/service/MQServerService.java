package com.fdu.synserver.service;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

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
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.tomcat.util.http.fileupload.ByteArrayOutputStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fdu.synserver.entity.ChainEventMessage;
import com.fdu.synserver.entity.SignatureMessage;

import jakarta.annotation.PostConstruct;

//与MQServer端的交互逻辑
//如果是Client就把计算得到的签名发给MQServer端
//如果是Server就用PushComusmer接收消息,一个是接收Client的Signature，一个是接收区块链监听器
@Service
public class MQServerService {
    private static final Log LOG = LogFactory.getLog(MQClientService.class);

    @org.springframework.beans.factory.annotation.Value("${my.custom.config.isServer}")
    private boolean isServer;

    @org.springframework.beans.factory.annotation.Value("${spring.rocketmqServer.proxy.server-address}")
    private String proxyServerAddress;

    @org.springframework.beans.factory.annotation.Value("${spring.rocketmqServer.topic.syn}")
    private String synTopic;

    @org.springframework.beans.factory.annotation.Value("${spring.rocketmqServer.topic.signature}")
    private String signatureTopic;

    @Autowired
    private ObjectMapper objectMapper;

    private Producer signatureProducer;
    private ClientServiceProvider provider;
    private ClientConfiguration clientConfiguration;
    private PushConsumer pushConsumer;
    private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(Kryo::new);

    private final DBService dbService;
    private final SignatureService signatureService;

    public MQServerService(DBService dbService, SignatureService signatureService) {
        this.dbService = dbService;
        this.signatureService = signatureService;
    }

    @PostConstruct
    public void initProducer() throws ClientException {
        // 测试的时候就是!isServer
        if (isServer) {
            return;
        }
        provider = ClientServiceProvider.loadService();
        clientConfiguration = ClientConfiguration.newBuilder()
                .setEndpoints(proxyServerAddress)
                .build();
        ClientConfigurationBuilder builder = ClientConfiguration.newBuilder().setEndpoints(proxyServerAddress);
        ClientConfiguration configuration = builder.build();
        signatureProducer = provider.newProducerBuilder()
                .setClientConfiguration(configuration)
                .build();
    }

    // 可信任Client发送消息给Server
    public void sendSignature(SignatureMessage signatureMessage) {
        if (isServer) {
            return;
        }
        Kryo kryo = kryoThreadLocal.get();
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream(); // 重用字节输出流
        Output output = new Output(byteOut); // 重用 Kryo 的 Output 对象
        kryo.writeObject(output, signatureMessage);
        output.flush();

        byte[] serializedBytes = byteOut.toByteArray(); // 获取序列化后的字节数组

        Message message = provider.newMessageBuilder()
                .setTopic(signatureTopic)
                .setTag(signatureMessage.getDbName())
                .setBody(serializedBytes)
                .build();
        try {
            // 发送消息，需要关注发送结果，并捕获失败等异常。
            SendReceipt sendReceipt = signatureProducer.send(message);
            LOG.debug("Send message successfully, messageId=" + sendReceipt.getMessageId()
                    + " topic = " + signatureTopic
                    + " tag=" + signatureMessage.getDbName());

        } catch (ClientException e) {
            LOG.error("Failed to send message", e);
        }
    }

    // 初始化服务端消费者消费signature
    @PostConstruct
    public void initSynConsumer() throws ClientException {
        if (!isServer) {
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
                    processKVMessage(messageView);
                    return ConsumeResult.SUCCESS;
                })
                .build();
    }

    @PostConstruct
    public void initSignatureConsumer() throws ClientException {
        if (!isServer) {
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
                .setConsumerGroup("signature_consumer") // 设置 Consumer Group
                .setSubscriptionExpressions(Collections.singletonMap(signatureTopic, filterExpression))
                .setMessageListener(messageView -> {
                    processSignatureMessage(messageView);
                    return ConsumeResult.SUCCESS;
                })
                .build();
    }

    // 接收区块链监听器的消息进行同步
    public void processSynMessage(MessageView messageView) {
        if (!isServer) {
            return;
        }
        LOG.debug("Received message: " + messageView.getMessageId());
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

        LOG.debug("add to buildSQ:" + chainEventMessage.toString());
        // 构造SQL并将语句加入待执行队列
        String sql = dbService.buildSQL(chainEventMessage);
        dbService.addSQL(sql);
    }

    // 接收Client的签名消息，批量刷入数据库
    public void processSignatureMessage(MessageView messageView) {
        if (!isServer) {
            return;
        }
        LOG.debug("Received message: " + messageView.getMessageId());
        ByteBuffer body = messageView.getBody();
        Kryo kryo = kryoThreadLocal.get();
        byte[] byteArray = new byte[body.remaining()];
        // 使用 body 的只读缓冲区创建一个副本，并将其内容读入 byteArray
        body.duplicate().get(byteArray);

        // 将字节数组包装成 Input 对象
        Input input = new Input(byteArray);
        SignatureMessage signatureMessage;
        try {
            signatureMessage = kryo.readObject(input, SignatureMessage.class);
            // build sql to update signature
            dbService.addSQL("UPDATE " + signatureMessage.getTableName() + " SET `signature` = '"
                    + signatureMessage.getSignature() + "' WHERE `key` = '" + signatureMessage.getKey() + "'");
        } catch (Exception e) {
            LOG.error("Failed to deserialize message: " + messageView.getMessageId());
            input.close();
            return;
        }

    }

    public void processKVMessage(MessageView messageView) {
        // 提取 Tag 和 Keys
        String tag = messageView.getTag().orElse(null); // channelName
        String keys = messageView.getProperties().get(MessageConst.PROPERTY_KEYS);

        // 反序列化消息主体
        ByteBuffer body = messageView.getBody();
        byte[] byteArray = new byte[body.remaining()];
        body.get(byteArray);

        // 解析消息为 Map
        Map<String, String> producerMap;
        try {
            producerMap = objectMapper.readValue(byteArray, Map.class);
        } catch (Exception ex) {
            LOG.error("Failed to parse message: " + messageView.getMessageId(), ex);
            return;
        }

        // 构造 ChainEventMessage 对象
        try {
            ChainEventMessage chainEventMessage = ChainEventMessage.builder()
                    .chainType(producerMap.get("chainType"))
                    .channelName(producerMap.get("channelName"))
                    .key(producerMap.get("key"))
                    .value(producerMap.get("value"))
                    .updateTime(Long.valueOf(producerMap.get("updateTime")))
                    .build();
            LOG.debug("Processed message successfully: " + chainEventMessage.toString());
            LOG.debug("Add to buildSQL:" + chainEventMessage.toString());
            // 构造SQL并将语句加入待执行队列
            String sql = dbService.buildSQL(chainEventMessage);
            dbService.addSQL(sql);
        } catch (Exception ex) {
            LOG.error("Failed to process message: " + messageView.getMessageId(), ex);
        }

    }
}
