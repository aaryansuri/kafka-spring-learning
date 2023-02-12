package com.mykafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mykafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventsConsumer {


    @Autowired
    LibraryEventsService service;

    @KafkaListener(topics = {"library-events"},
        groupId = "library-events-listener-group"
    )
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {

        log.info("Consumer Record : {}", consumerRecord);
        service.processLibraryEvent(consumerRecord);

    }
}
