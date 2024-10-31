package com.fdu.synserver.schedule;

import java.util.UUID;

import javax.annotation.PostConstruct;

import org.apache.rocketmq.client.apis.ClientException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.fdu.synserver.entity.*;
import com.fdu.synserver.service.MQServerService;
import com.fdu.synserver.service.MQSynTestService;
@Component
public class ScheduleService {

    private final MQSynTestService mqSynTestService;

    public ScheduleService(MQSynTestService mqSynTestService) {
        this.mqSynTestService = mqSynTestService;
    }

    //@Scheduled(fixedRate = 1000)
    public void testSyn() {
        ChainEventMessage chainEventMessage = new ChainEventMessage();
        String uuid = UUID.randomUUID().toString();
        Long curTime = System.currentTimeMillis();
        chainEventMessage.setKey(uuid);
        chainEventMessage.setChainType("fabric");
        chainEventMessage.setChannelName("test_channel");
        chainEventMessage.setValue(uuid);
        chainEventMessage.setOperationType((byte) 1);
        chainEventMessage.setUpdateTime(curTime);
        mqSynTestService.sendChainEventMessage(chainEventMessage);
    }   
    
}
