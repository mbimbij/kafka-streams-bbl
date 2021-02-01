package com.example.demo.windowing.sliding;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static com.example.demo.windowing.sliding.HoppingWindowSumSuppressed.INPUT_TOPIC;
import static com.example.demo.windowing.sliding.HoppingWindowSumSuppressed.OUTPUT_TOPIC;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.springframework.kafka.support.KafkaHeaders.TOPIC;

@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@ContextConfiguration(classes = HoppingWindowSumSuppressedIT.ConfigurationIT.class)
@EmbeddedKafka(topics = {INPUT_TOPIC, OUTPUT_TOPIC})
@ActiveProfiles("test")
@TestInstance(PER_CLASS)
@Slf4j
class HoppingWindowSumSuppressedIT {
  @Autowired
  private KafkaTemplate<String, String> producer;
  @Autowired
  private EmbeddedKafkaBroker embeddedKafkaBroker;
  @Autowired
  private TestListenerIT listener;
  private KafkaStreams kafkaStreams;
  private static final int TOLERATION_WINDOW_SECONDS = 3;

  @BeforeAll
  void beforeAll() {
    HoppingWindowSumSuppressed streamApplication = new HoppingWindowSumSuppressed(embeddedKafkaBroker.getBrokersAsString(), TOLERATION_WINDOW_SECONDS);
    Topology topology = streamApplication.getTopology();
    Properties properties = streamApplication.getProperties();
    kafkaStreams = new KafkaStreams(topology, properties);
    kafkaStreams.start();
    await().atMost(Duration.ofSeconds(10))
        .until(() -> listener.isReady());
  }

  @AfterAll
  void afterAll() {
    kafkaStreams.close();
    kafkaStreams.cleanUp();
  }

  @BeforeEach
  void setUp() {
    listener.clear();
  }

  @Test
  public void shouldNotJoin_whenSecondEventArrivesAfterTheTolerationWindow() {
    // given
    String expectedRecord = String.join(",", "v1-1", "v1-2");
    long nowMillis = Instant.now().toEpochMilli();
    log.info("expected record: {}", expectedRecord);
    long delaySeconds = TOLERATION_WINDOW_SECONDS + 1;
    Message<String> message1 = MessageBuilder
        .withPayload("v1-1")
        .setHeader(KafkaHeaders.MESSAGE_KEY, "k1")
        .setHeader(KafkaHeaders.TIMESTAMP, nowMillis)
        .setHeader(TOPIC, INPUT_TOPIC)
        .build();

    // when
    producer.send(message1);

    // then - waiting for the join result throws an exception - since it is outside of the toleration window
    assertThatThrownBy(() -> await()
        .atMost(Duration.ofSeconds(3))
        .until(() -> listener.getByKey("k1"), list -> list.contains(expectedRecord)));
  }

  @SpringBootConfiguration
  @EnableAutoConfiguration
  @ComponentScan(basePackages = "com.example.demo")
  public static class ConfigurationIT {
  }

  @Slf4j
  @Component
  public static class TestListenerIT implements ConsumerSeekAware {
    private boolean ready = false;
    private Map<String, List<String>> receivedRecords = new HashMap<>();

    @KafkaListener(topics = OUTPUT_TOPIC)
    private void consumer(ConsumerRecord<String, String> record) {
      log.info("received {}", record);
      String recordKey = record.key();
      receivedRecords.putIfAbsent(recordKey, new ArrayList<>());
      receivedRecords.get(recordKey).add(record.value());
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      ready = false;
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
      ready = true;
    }

    public void clear() {
      receivedRecords.clear();
    }

    public boolean isReady() {
      return ready;
    }

    public List<String> getByKey(String key){
      receivedRecords.putIfAbsent(key, new ArrayList<>());
      return receivedRecords.get(key);
    }
  }
}