package com.zijinph.platform.rocketmq.client;

import com.zijinph.platform.rocketmq.config.RocketConfig;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

public class Producer extends DefaultMQProducer {

    private RocketConfig config;

    public Producer(RocketConfig config) {
        super(config.getProducerGroupName(), new AclClientRPCHook(new SessionCredentials(config.getAccessKey(), config.getSecretKey())));
        setNamesrvAddr(config.getNamesrvAddr());
        setSendMsgTimeout(config.getSendMsgTimeout());

        this.config = config;
    }

}
