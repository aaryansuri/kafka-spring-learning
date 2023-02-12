package com.mykafka.domain;

import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class LibraryEvent {
    private Integer libraryEventId;
    @NotNull
    @Valid
    private Book book;
    private LibraryEventType libraryEventType;
}
