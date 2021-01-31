package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static com.example.demo.MultiPartitionJoinWithTimeToleration.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.springframework.kafka.support.KafkaHeaders.TOPIC;

@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@ContextConfiguration(classes = ConfigurationIT.class)
@EmbeddedKafka(topics = {INPUT_TOPIC_A, INPUT_TOPIC_B, OUTPUT_TOPIC})
@ActiveProfiles("test")
@TestInstance(PER_CLASS)
@Slf4j
class MultiPartitionJoinWithTimeTolerationIT {
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
    MultiPartitionJoinWithTimeToleration streamApplication = new MultiPartitionJoinWithTimeToleration(embeddedKafkaBroker.getBrokersAsString(), TOLERATION_WINDOW_SECONDS);
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

  @ParameterizedTest
  @CsvSource({
      "0,  v1-1, v2-1",
      (TOLERATION_WINDOW_SECONDS - 1) + ", toto, tata"
  })
  public void shouldJoin_whenSecondEventArrivesWithinTheTolerationWindow(int delaySeconds, String valueFirstRecord, String valueSecondRecord) {
    // given
    String expectedRecord = String.join(",", valueFirstRecord, valueSecondRecord);
    long nowMillis = Instant.now().toEpochMilli();
    log.info("expected record: {}", expectedRecord);
    Message<String> message1 = MessageBuilder
        .withPayload(valueFirstRecord)
        .setHeader(KafkaHeaders.MESSAGE_KEY, "k1")
        .setHeader(KafkaHeaders.TIMESTAMP, nowMillis)
        .setHeader(TOPIC, INPUT_TOPIC_A)
        .build();
    Message<String> message2 = MessageBuilder
        .withPayload(valueSecondRecord)
        .setHeader(KafkaHeaders.MESSAGE_KEY, "k1")
        .setHeader(KafkaHeaders.TIMESTAMP, nowMillis + delaySeconds * 1000L)
        .setHeader(TOPIC, INPUT_TOPIC_B)
        .build();

    // when
    producer.send(message1);
    producer.send(message2);
//    producer.send(INPUT_TOPIC_A, "k1", valueFirstRecord);
//    Thread.sleep(delaySeconds * 1000L);
//    producer.send(INPUT_TOPIC_B, "k1", valueSecondRecord);

    // then
    List<String> joinedRecords = await()
        .atMost(Duration.ofSeconds(3))
        .until(() -> listener.getByKey("k1"), list -> list.contains(expectedRecord));
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
        .setHeader(TOPIC, INPUT_TOPIC_A)
        .build();
    Message<String> message2 = MessageBuilder
        .withPayload("v1-2")
        .setHeader(KafkaHeaders.MESSAGE_KEY, "k1")
        .setHeader(KafkaHeaders.TIMESTAMP, nowMillis + delaySeconds * 1000L)
        .setHeader(TOPIC, INPUT_TOPIC_B)
        .build();

    // when
    producer.send(message1);
    producer.send(message2);

    // then - waiting for the join result throws an exception - since it is outside of the toleration window
    assertThatThrownBy(() -> await()
        .atMost(Duration.ofSeconds(3))
        .until(() -> listener.getByKey("k1"), list -> list.contains(expectedRecord)));
  }
}