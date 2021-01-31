package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.SocketUtils;

import java.time.Duration;
import java.util.Map;

import static org.awaitility.Awaitility.await;

@Configuration
@SpringBootTest
@ContextConfiguration(
    classes = {MultiPartitionJoinWithTimeTolerationIT.class, KafkaAutoConfiguration.class}
    , initializers = MultiPartitionJoinWithTimeTolerationIT.class
)
@EmbeddedKafka
    (
        partitions = 2,
        topics = {MultiPartitionJoinWithTimeToleration.INPUT_TOPIC_A, MultiPartitionJoinWithTimeToleration.INPUT_TOPIC_B, MultiPartitionJoinWithTimeToleration.OUTPUT_TOPIC}
    )
@ActiveProfiles("test")
@Slf4j
class MultiPartitionJoinWithTimeTolerationIT implements ConsumerSeekAware, ApplicationContextInitializer<ConfigurableApplicationContext> {
  @Autowired
  private KafkaTemplate<String, String> template;
  private boolean consumerReady = false;
  @Autowired
  private EmbeddedKafkaBroker embeddedKafkaBroker;

  @KafkaListener(topics = MultiPartitionJoinWithTimeToleration.OUTPUT_TOPIC)
  private void consumer(String record) {
    log.info("received {}", record);
  }

  @Override
  public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
    consumerReady = true;
  }

  @Test
  public void givenEmbeddedKafkaBroker_whenSendingtoSimpleProducer_thenMessageReceived()
      throws Exception {
    await().atMost(Duration.ofSeconds(10))
        .until(() -> this.consumerReady);
    Topology topology = MultiPartitionJoinWithTimeToleration.getTopology();
    System.out.println();
  }

  @Override
  public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
    String embeddedKafkaAddress = configurableApplicationContext.getEnvironment().getProperty("spring.embedded.kafka.brokers");
    TestPropertyValues.of(
        String.format("spring.kafka.bootstrap-servers[0]=%s", embeddedKafkaAddress)
    ).applyTo(configurableApplicationContext.getEnvironment());
  }
}