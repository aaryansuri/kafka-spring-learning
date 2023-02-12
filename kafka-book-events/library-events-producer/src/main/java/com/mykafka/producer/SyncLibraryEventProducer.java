package com.mykafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mykafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

@Component("sync")
@Slf4j
public class SyncLibraryEventProducer extends LibraryEventProducer {

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Override
    public void sendLibraryEvent(LibraryEvent libraryEvent) throws Exception {

        SendResult<Integer, String> sendResult = send(libraryEvent);
        log.info("SendResult is {}", sendResult.toString());
    }

    private SendResult<Integer, String> send(LibraryEvent libraryEvent) throws Exception {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        SendResult<Integer, String> sendResult = null;

        try {
            sendResult = kafkaTemplate.sendDefault(key, value).get();
        } catch (ExecutionException | InterruptedException e) {
            log.error("ExecutionException / InterruptedException Sending the message, exception={}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Exception Sending the message, exception={}", e.getMessage());
            throw e;
        }

        return sendResult;
    }


}
