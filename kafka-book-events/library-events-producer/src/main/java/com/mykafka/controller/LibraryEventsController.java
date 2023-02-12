package com.mykafka.controller;

import com.mykafka.domain.LibraryEvent;
import com.mykafka.domain.LibraryEventType;
import com.mykafka.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

@RestController
@Slf4j
public class LibraryEventsController {

    @Autowired
    @Qualifier("simple")
    LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/library-event")
    public ResponseEntity<LibraryEvent> addLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws Exception {

        log.info("starts sending");
        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        libraryEventProducer.sendLibraryEvent(libraryEvent);
        log.info("after sending");

        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/v1/library-event")
    public ResponseEntity<?> updateLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws Exception {

        if(libraryEvent.getLibraryEventId() == null)
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("can't pass id as null while updating");

        log.info("starts sending");
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEventProducer.sendLibraryEvent(libraryEvent);
        log.info("after sending");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}
