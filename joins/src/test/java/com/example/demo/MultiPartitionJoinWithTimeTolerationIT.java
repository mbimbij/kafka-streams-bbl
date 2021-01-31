package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static com.example.demo.MultiPartitionJoinWithTimeToleration.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@SpringBootTest
@ContextConfiguration(
    classes = {ConfigurationIT.class}
    , initializers = MultiPartitionJoinWithTimeTolerationIT.class
)
@EmbeddedKafka(
    topics = {INPUT_TOPIC_A, INPUT_TOPIC_B, OUTPUT_TOPIC}
)
@ActiveProfiles("test")
@TestInstance(PER_CLASS)
@Slf4j
class MultiPartitionJoinWithTimeTolerationIT implements ApplicationContextInitializer<ConfigurableApplicationContext> {
  @Autowired
  private KafkaTemplate<String, String> producer;
  @Autowired
  private EmbeddedKafkaBroker embeddedKafkaBroker;
  @Autowired
  private TestListenerIT listener;
  private KafkaStreams kafkaStreams;
  private final int tolerationWindowSeconds = 3;

  @Override
  public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
    String embeddedKafkaAddress = configurableApplicationContext.getEnvironment().getProperty("spring.embedded.kafka.brokers");
    TestPropertyValues.of(
        String.format("spring.kafka.bootstrap-servers[0]=%s", embeddedKafkaAddress)
    ).applyTo(configurableApplicationContext.getEnvironment());
  }

  @BeforeAll
  void beforeAll() {
    MultiPartitionJoinWithTimeToleration streamApplication = new MultiPartitionJoinWithTimeToleration(embeddedKafkaBroker.getBrokersAsString(), tolerationWindowSeconds);
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
      "2, toto, tata"
  })
  public void shouldJoin_whenSecondEventArrivesWithinTheTolerationWindow(int delaySeconds, String valueFirstRecord, String valueSecondRecord)
      throws Exception {
    // given
    String expectedRecord = String.join(",", valueFirstRecord, valueSecondRecord);
    log.info("expected record: {}", expectedRecord);

    // when
    producer.send(INPUT_TOPIC_A, "k1", valueFirstRecord);
    Thread.sleep(delaySeconds * 1000L);
    producer.send(INPUT_TOPIC_B, "k1", valueSecondRecord);

    // then
    List<String> joinedRecords = await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> listener.getByKey("k1"), list -> list.contains(expectedRecord));
  }
}