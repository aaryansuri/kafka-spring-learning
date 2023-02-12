package com.mykafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mykafka.domain.LibraryEvent;
import com.mykafka.handlers.StatusHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;

@Component("record-header")
@Slf4j
public class RecordHeadersLibraryEventProducer extends RecordLibraryEventProducer {

    @Value("${spring.kafka.template.default-topic}")
    String topic;

    @Override
    public void sendLibraryEvent(LibraryEvent libraryEvent) throws Exception {
        super.sendLibraryEvent(libraryEvent);
    }

    public ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {
        List<Header> recordHeaders = List.of(
                new RecordHeader("event-source", "scanner".getBytes()),
                new RecordHeader("random-header", "random".getBytes())
        );
        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

}
