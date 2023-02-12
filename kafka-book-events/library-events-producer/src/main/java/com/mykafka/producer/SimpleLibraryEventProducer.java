package com.mykafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mykafka.domain.LibraryEvent;
import com.mykafka.handlers.StatusHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Component("simple")
@Slf4j
public class SimpleLibraryEventProducer extends LibraryEventProducer {


    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;


    @Autowired
    StatusHandler handler;

    @Override
    public void sendLibraryEvent(LibraryEvent libraryEvent) throws Exception {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);

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

}
