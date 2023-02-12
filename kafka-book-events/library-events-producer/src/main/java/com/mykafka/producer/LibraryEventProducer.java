package com.mykafka.producer;

import com.mykafka.domain.LibraryEvent;


public abstract class LibraryEventProducer {
    public abstract void sendLibraryEvent(final LibraryEvent libraryEvent) throws Exception;
}
