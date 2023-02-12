package com.mykafka.handlers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class StatusHandler {
    public void fail(Integer key, String value, Throwable ex) {
        log.error("Error Sending the message - key : {} and value - {} and the exception is {}", key, value, ex.getMessage());
        try {
            throw ex;
        } catch (Throwable e) {
            log.error("Error in OnFailure : {}", e.getMessage());
        }
    }
    public void success(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message Sent Successfully for the key : {} and the value is {}, partition is {}", key, value, result.getRecordMetadata().partition());
    }

}
