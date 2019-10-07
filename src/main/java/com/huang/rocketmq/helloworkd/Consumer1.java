package com.huang.rocketmq.helloworkd;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author hsz
 */


public class Consumer1 {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-group");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe("test-topic", "*");
        consumer.setConsumeThreadMin(3);
        consumer.setConsumeThreadMax(3);
        consumer.registerMessageListener(new MessageListenerOrderly() {
             @Override
             public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                 System.out.println("消息内容：" + new String(msgs.get(0).getBody()) + "，队列：" + msgs.get(0).getQueueId() + "，线程：" + Thread.currentThread().getName());
                 return ConsumeOrderlyStatus.SUCCESS;
             }
         });
        consumer.start();
        System.out.println("开始消费");
    }
}
