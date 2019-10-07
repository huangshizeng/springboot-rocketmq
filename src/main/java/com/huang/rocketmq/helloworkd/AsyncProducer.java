package com.huang.rocketmq.helloworkd;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

/**
 * @author hsz
 */


public class AsyncProducer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-group");
        producer.setNamesrvAddr("localhost:9876");
//        producer.setRetryTimesWhenSendAsyncFailed(3);
        producer.start();
        for (int i = 0; i < 5; i++) {
            Message message = new Message("test-topic", "TagA", ("Hello Rocket" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    return mqs.get(0);
                }
            }, 0);
            System.out.println(sendResult);
        }
        for (int i = 5; i < 10; i++) {
            Message message = new Message("test-topic", "TagA", ("Hello Rocket" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    return mqs.get(1);
                }
            }, 0);
            System.out.println(sendResult);
        }
        producer.shutdown();
    }
}
