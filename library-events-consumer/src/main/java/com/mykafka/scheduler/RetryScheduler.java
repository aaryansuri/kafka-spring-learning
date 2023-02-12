package com.mykafka.scheduler;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.mykafka.entity.FailureRecord;
import com.mykafka.jpa.FailureRecordRepository;
import com.mykafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RetryScheduler {

    @Autowired
    private FailureRecordRepository repository;

    @Autowired
    private LibraryEventsService service;

    @Scheduled(fixedRate = 10000)
    public void retryFailedRecords() {

        log.info("Retrying saved failed records");

        repository.findAllByStatus("RETRY")
                .forEach(entry -> {
                    try {

                        var record = buildConsumerRecord(entry);
                        service.processLibraryEvent(record);
                        entry.setStatus("SUCCESS");

                    } catch (JsonProcessingException e) {
                        log.error("Exception in processing saved record again : {}", e.getMessage(), e);
                    }
                });

        log.info("Retrying saved failed done");
    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord record) {
        return new ConsumerRecord<>(
                record.getTopic(),
                record.getPartition(),
                record.getOffset_value(),
                record.getConsumerKey(),
                record.getErrorRecord()
        );
    }
}
