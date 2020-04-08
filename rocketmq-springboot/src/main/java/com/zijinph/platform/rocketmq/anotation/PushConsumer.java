package com.zijinph.platform.rocketmq.anotation;

import org.springframework.stereotype.Component;

import java.lang.annotation.*;

@Documented
@Component
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface PushConsumer {

    String topic();

    int consumerBatchMaxSize() default 1;
}
