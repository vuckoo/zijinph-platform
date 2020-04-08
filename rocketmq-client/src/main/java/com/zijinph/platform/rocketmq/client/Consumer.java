package com.zijinph.platform.rocketmq.client;

import com.zijinph.platform.rocketmq.config.RocketConfig;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

public class Consumer extends DefaultMQPushConsumer {

    private RocketConfig config;

    public Consumer(RocketConfig config) {
        super(config.getConsumerGroupName(),
                new AclClientRPCHook(new SessionCredentials(config.getAccessKey(), config.getSecretKey())),
                new AllocateMessageQueueAveragely());
        setNamesrvAddr(config.getNamesrvAddr());

        // 默认从最小的offset开始消费
        setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        this.config = config;
    }

    public void subscribe(String topic) throws MQClientException {
        super.subscribe(topic, "*");
    }
}
