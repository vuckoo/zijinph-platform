package com.zijinph.platform.rocketmq.config;

import lombok.Data;

/**
 * @author zhangpeipei
 * @date 2020/4/8 14:21
 */
@Data
public class RocketConfig {

    private String accessKey;
    private String secretKey;

    private String namesrvAddr;
    private String producerGroupName;
    private int sendMsgTimeout = 3000;

    private String consumerGroupName;
    private int consumerBatchMaxSize;

}
