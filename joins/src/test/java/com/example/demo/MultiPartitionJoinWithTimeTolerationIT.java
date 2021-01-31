package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerSeekAware;
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
//    ,initializers = MultiplePartitionsJoinsTest.class
)
@EmbeddedKafka
    (
    partitions = 2,
    brokerProperties = {"listeners=PLAINTEXT://localhost:${kafka.port}", "port=${kafka.port}"},
    topics = {"${test.topic}"}
)
@ActiveProfiles("test")
@Slf4j
class MultiPartitionJoinWithTimeTolerationIT implements ConsumerSeekAware, ApplicationContextInitializer<ConfigurableApplicationContext> {
  @Value("${test.topic}")
  private String topic;
  @Autowired
  private KafkaTemplate<String, String> template;
  private boolean consumerReady = false;

  @KafkaListener(topics = "${test.topic}")
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
    template.send(topic, "hello world");
    System.out.println();
  }

  @Override
  public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
    int embeddedKafkaPort = SocketUtils.findAvailableTcpPort();
    TestPropertyValues.of(
        String.format("spring.kafka.bootstrap-servers[0]=http://localhost:%d", embeddedKafkaPort ),
        String.format("kafka.port=%d", embeddedKafkaPort)
    ).applyTo(configurableApplicationContext.getEnvironment());
  }
}