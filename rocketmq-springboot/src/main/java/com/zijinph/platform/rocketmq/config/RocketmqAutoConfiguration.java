package com.zijinph.platform.rocketmq.config;

import com.zijinph.platform.rocketmq.anotation.PushConsumer;
import com.zijinph.platform.rocketmq.client.Producer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import static com.zijinph.platform.rocketmq.consts.Const.APPLICATION_KEY;

@Slf4j
@Configuration
@EnableConfigurationProperties(RocketmqProperties.class)
@ConditionalOnProperty(prefix = RocketmqProperties.PREFIX, value = "namesrvAddr")
public class RocketmqAutoConfiguration {

    @Autowired
    private RocketmqProperties properties;

    @Autowired
    Environment environment;


    /**
     * 初始化向rocketmq发送普通消息的生产
     * 一个应用创建一个Producer，由应用来维护此对象，可以设置为全局对象或者单例<br>
     * 注意：ProducerGroupName需要由应用来保证唯一<br>
     * ProducerGroup这个概念发送普通的消息时，作用不大，但是发送分布式事务消息时，比较关键，
     * 因为服务器会回查这个Group下的任意一个Producer
     */
    @Bean
    @ConditionalOnProperty(prefix = RocketmqProperties.PREFIX, value = "producerGroupName")
    public Producer defaultProducer() throws MQClientException {
        RocketConfig config = new RocketConfig();
        config.setAccessKey(properties.getAccessKey());
        config.setAccessKey(properties.getSecretKey());
        config.setProducerGroupName(properties.getProducerGroupName());
        config.setSendMsgTimeout(properties.getSendMsgTimeout());

        Producer producer = new Producer(config);
        producer.setInstanceName(properties.getProducerGroupName());
        producer.setVipChannelEnabled(false);
        producer.setRetryTimesWhenSendAsyncFailed(10);
        producer.start();
        log.info("RocketMq defaultProducer Started.");
        return producer;
    }

    private AclClientRPCHook getAcl(RocketmqProperties properties) {
        String accessKey = properties.getAccessKey();
        if (StringUtils.isEmpty(accessKey)) {
            accessKey = environment.getProperty(APPLICATION_KEY);
        }
        String secretKey = properties.getSecretKey();
        if (StringUtils.isEmpty(accessKey) || StringUtils.isEmpty(secretKey)) {
            throw new RuntimeException("accessKey, secretKey must not be null");
        }
        return new AclClientRPCHook(new SessionCredentials(secretKey, secretKey));
    }

    @Bean
    @ConditionalOnProperty(prefix = RocketmqProperties.PREFIX, value = "consumerGroupName")
    public BeanPostProcessor consumerRegister() {
        log.info("register rocket consumer in BeanPostProcessor.");
        BeanPostProcessor processor = new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(final Object bean, final String beanName) throws BeansException {
                Class<?> targetClass = AopUtils.getTargetClass(bean);
                if (targetClass.isAnnotationPresent(PushConsumer.class)) {
                    try {
                        registerConsumer(bean);
                        log.info("register consumer listener : {}", bean.getClass().getName());
                    } catch (Exception e) {
                        log.error("register consumer listener error : {}", bean.getClass().getName(), e);
                        throw new RuntimeException("rocketmq listener register fail, check configuration!");
                    }
                }

                return bean;
            }
        };

        return processor;
    }

    private DefaultMQPushConsumer registerConsumer(Object bean) throws MQClientException {
        PushConsumer conusmeAno = bean.getClass().getAnnotation(PushConsumer.class);
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(properties.getConsumerGroupName(),
                getAcl(properties), new AllocateMessageQueueAveragely());
        consumer.setNamesrvAddr(properties.getNamesrvAddr());
        consumer.setInstanceName(properties.getConsumerGroupName());
        // 设置批量消费，以提升消费吞吐量，默认是1
        int consumeBatchSize = 1;
        if (conusmeAno.consumerBatchMaxSize() > 1) {
            consumeBatchSize = conusmeAno.consumerBatchMaxSize();
        } else {
            consumeBatchSize = properties.getConsumerBatchMaxSize();
        }
        consumer.setConsumeMessageBatchMaxSize(consumeBatchSize);
        /**
         * 订阅指定topic下tags
         */
        String topicAndTag = conusmeAno.topic();
        String[] topicArr = topicAndTag.split(",");
        for (String sunscribe : topicArr) {
            String[] topicTag = sunscribe.split(":");
            if (topicTag.length == 1) {
                consumer.subscribe(topicTag[0], "*");
            } else {
                consumer.subscribe(topicTag[0], topicTag[1]);
            }
        }
        consumer.registerMessageListener((MessageListenerConcurrently) bean);
        consumer.start();

        return consumer;
    }


}