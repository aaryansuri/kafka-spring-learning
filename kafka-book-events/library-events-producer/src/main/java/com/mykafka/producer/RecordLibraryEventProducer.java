package com.mykafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mykafka.domain.LibraryEvent;
import com.mykafka.handlers.StatusHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Component("record")
@Slf4j
public class RecordLibraryEventProducer extends LibraryEventProducer{

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;
    @Autowired
    ObjectMapper objectMapper;

    @Value("${spring.kafka.template.default-topic}")
    String topic;

    @Autowired
    StatusHandler handler;
    @Override
    public void sendLibraryEvent(LibraryEvent libraryEvent) throws Exception {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, topic);

        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);
        listenableFuture.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable ex) {
                handler.fail(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handler.success(key, value, result);
            }
        });

    }

    protected ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {
        return new ProducerRecord<>(topic, null, key, value, null);
    }
}
