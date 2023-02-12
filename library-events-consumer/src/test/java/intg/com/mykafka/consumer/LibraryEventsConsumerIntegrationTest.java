package com.mykafka.consumer;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mykafka.entity.Book;
import com.mykafka.entity.LibraryEvent;
import com.mykafka.entity.LibraryEventType;
import com.mykafka.jpa.FailureRecordRepository;
import com.mykafka.jpa.LibraryEventsRepository;
import com.mykafka.service.LibraryEventsService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events", "library-events.RETRY", "library-events.DLT"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}", "retryListener.startup:false"})
public class LibraryEventsConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;


    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    LibraryEventsService libraryEventsServiceSpy;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    @Autowired
    FailureRecordRepository failureRecordRepository;

    @Autowired
    ObjectMapper objectMapper;

    private Consumer<Integer, String> consumer;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    @BeforeEach
    void setUp() {

        var container = endpointRegistry.getListenerContainers()
                                   .stream()
                                   .filter(messageListenerContainer -> Objects.equals(messageListenerContainer.getGroupId(), "library-events-listener-group"))
                                   .collect(Collectors.toList()).get(0);

        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());


    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent()
        throws ExecutionException, InterruptedException, JsonProcessingException {
        String json = "{\n"
            + "    \"libraryEventId\" : \"1\",\n"
            + "    \"libraryEventType\": \"NEW\",\n"
            + "    \"book\" : {\n"
            + "        \"bookId\" : 46,\n"
            + "        \"bookName\" : \"the lost doggy\",\n"
            + "        \"bookAuthor\" : \"duggy123\"\n"
            + "    }\n"
            + "}";

        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assert libraryEventList.size() == 1;
        libraryEventList.forEach(libraryEvent -> {
            assert  libraryEvent.getLibraryEventId() != null;
            assertEquals(46, libraryEvent.getBook().getBookId());
        });
    }

    @Test
    void publishUpdateLibraryEvent()
        throws JsonProcessingException, ExecutionException, InterruptedException {

        String json = "{\n"
            + "    \"libraryEventId\" : \"null\",\n"
            + "    \"eventStatus\": \"ADD\",\n"
            + "    \"book\" : {\n"
            + "        \"bookId\" : 46,\n"
            + "        \"bookName\" : \"the lost doggy\",\n"
            + "        \"bookAuthor\" : \"duggy123\"\n"
            + "    }\n"
            + "}";

        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        Book updatedBook = Book.builder().bookId(46).bookName("the lost doggy 2.0").bookAuthor("duggyMuggy").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        LibraryEvent savedLibraryEvent = libraryEventsRepository.findById(
            libraryEvent.getLibraryEventId()).get();

        assertEquals("the lost doggy 2.0", savedLibraryEvent.getBook().getBookName());

    }

    @Test
    void publishUpdateCustomErrorLibraryEvent_RetryTopic()
        throws JsonProcessingException, ExecutionException, InterruptedException {

        String json = "{\n"
            + "    \"libraryEventId\" : \"1000\",\n"
            + "    \"libraryEventType\": \"UPDATE\",\n"
            + "    \"book\" : {\n"
            + "        \"bookId\" : 46,\n"
            + "        \"bookName\" : \"the lost doggy\",\n"
            + "        \"bookAuthor\" : \"duggy123\"\n"
            + "    }\n"
            + "}";

        kafkaTemplate.sendDefault(json).get();


        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1","true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, retryTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, retryTopic);
        assertEquals(json, consumerRecord.value());

    }

    @Test
    void publishUpdateNullErrorLibraryEvent_DeadTopic()
            throws JsonProcessingException, ExecutionException, InterruptedException {

        String json = "{\n"
                + "    \"libraryEventId\" : \"null\",\n"
                + "    \"libraryEventType\": \"UPDATE\",\n"
                + "    \"book\" : {\n"
                + "        \"bookId\" : 46,\n"
                + "        \"bookName\" : \"the lost doggy\",\n"
                + "        \"bookAuthor\" : \"duggy123\"\n"
                + "    }\n"
                + "}";

        kafkaTemplate.sendDefault(json).get();


        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group2","true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, deadLetterTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, deadLetterTopic);
        assertEquals(json, consumerRecord.value());

    }

    @Test
    void publishUpdateNullFailureLibraryEvent_DB()
        throws JsonProcessingException, ExecutionException, InterruptedException {

        String json = "{\n"
            + "    \"libraryEventId\" : \"1000\",\n"
            + "    \"libraryEventType\": \"UPDATE\",\n"
            + "    \"book\" : {\n"
            + "        \"bookId\" : 46,\n"
            + "        \"bookName\" : \"the lost doggy\",\n"
            + "        \"bookAuthor\" : \"duggy123\"\n"
            + "    }\n"
            + "}";

        kafkaTemplate.sendDefault(json).get();


        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));


        var count = failureRecordRepository.count();
        assertEquals(1, count);

        failureRecordRepository.findAll().forEach(System.out::println);

    }
}
