package com.mykafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

//@Component
@Slf4j
public class LibraryEventsManualOffsetConsumer implements AcknowledgingMessageListener<Integer, String> {


    /**
     *  kafka.listener.ack-mode: manual
     * @param consumerRecord
     * @param acknowledgment
     */
    @Override
    @KafkaListener(topics = "library-events")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("ConsumerRecord ack : {}", consumerRecord);
        acknowledgment.acknowledge();
    }
}
