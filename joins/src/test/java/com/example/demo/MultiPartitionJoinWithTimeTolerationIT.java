package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import java.util.Properties;

import static com.example.demo.MultiPartitionJoinWithTimeToleration.*;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@ContextConfiguration(
    classes = {ConfigurationIT.class}
    , initializers = MultiPartitionJoinWithTimeTolerationIT.class
)
@EmbeddedKafka(
    topics = {INPUT_TOPIC_A, INPUT_TOPIC_B, OUTPUT_TOPIC}
)
@ActiveProfiles("test")
@Slf4j
class MultiPartitionJoinWithTimeTolerationIT implements ApplicationContextInitializer<ConfigurableApplicationContext> {
  @Autowired
  private KafkaTemplate<String, String> producer;
  @Autowired
  private EmbeddedKafkaBroker embeddedKafkaBroker;
  @Autowired
  private TestListener listener;
  private KafkaStreams kafkaStreams;

  @Override
  public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
    String embeddedKafkaAddress = configurableApplicationContext.getEnvironment().getProperty("spring.embedded.kafka.brokers");
    TestPropertyValues.of(
        String.format("spring.kafka.bootstrap-servers[0]=%s", embeddedKafkaAddress)
    ).applyTo(configurableApplicationContext.getEnvironment());
  }

  @BeforeEach
  void setUp() {
    MultiPartitionJoinWithTimeToleration streamApplication = new MultiPartitionJoinWithTimeToleration(embeddedKafkaBroker.getBrokersAsString());
    Topology topology = MultiPartitionJoinWithTimeToleration.getTopology();
    Properties properties = streamApplication.getProperties();
    kafkaStreams = new KafkaStreams(topology, properties);
    kafkaStreams.start();
    await().atMost(Duration.ofSeconds(10))
        .until(() -> listener.isReady());
    System.out.println();
  }

  @AfterEach
  void tearDown() {
    kafkaStreams.close();
    kafkaStreams.cleanUp();
  }

  @Test
  public void givenEmbeddedKafkaBroker_whenSendingtoSimpleProducer_thenMessageReceived()
      throws Exception {
    producer.send(OUTPUT_TOPIC, "hello world");
    await().atMost(Duration.ofSeconds(3))
        .until(() -> listener.getRecord("hello world").isPresent());
  }
}