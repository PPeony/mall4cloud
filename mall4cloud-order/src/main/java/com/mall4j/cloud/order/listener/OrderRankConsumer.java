package com.mall4j.cloud.order.listener;

import com.mall4j.cloud.common.cache.util.RedisUtil;
import com.mall4j.cloud.common.rocketmq.config.RocketMqConstant;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RocketMQMessageListener(topic = RocketMqConstant.ORDER_PRODUCT_RANK_TOPIC,consumerGroup = RocketMqConstant.ORDER_PRODUCT_RANK_CONSUMER_GROUP)
public class OrderRankConsumer implements RocketMQListener<List<Long>> {

    @Override
    public void onMessage(List<Long> longs) {
        System.out.println("======zset inc");
        for (Long id : longs) {
            RedisUtil.zInc(RocketMqConstant.PRODUCT_ZSET, 1, String.valueOf(id));
        }
    }
}
