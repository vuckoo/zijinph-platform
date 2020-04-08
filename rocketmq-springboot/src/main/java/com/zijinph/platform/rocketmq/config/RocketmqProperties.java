package com.zijinph.platform.rocketmq.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(RocketmqProperties.PREFIX)
public class RocketmqProperties {
    public static final String PREFIX = "rocketmq";

    //权限管理
    private String accessKey;
    private String secretKey;

    private String namesrvAddr;

    //生产者配置
    private String producerGroupName;
    private int sendMsgTimeout = 3000;

    //消费者配置
    private String consumerGroupName;
    private int consumerBatchMaxSize = 1;

}
