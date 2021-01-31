package com.example.demo;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class MultiPartitionJoinWithTimeTolerationTest {
  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopicA;
  private TestInputTopic<String, String> inputTopicB;
  private TestOutputTopic<String, String> outputTopic;
  private final Serde<String> stringSerde = new Serdes.StringSerde();

  @BeforeEach
  void setUp() {
    // set up properties for unit test
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "dummy");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy");
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());

    // set up TopologyTestDriver
    MultiPartitionJoinWithTimeToleration streamApplication = new MultiPartitionJoinWithTimeToleration("dummy", 10);
    testDriver = new TopologyTestDriver(streamApplication.getTopology(), properties);

    // setup test topics
    inputTopicA = testDriver.createInputTopic(MultiPartitionJoinWithTimeToleration.INPUT_TOPIC_A, stringSerde.serializer(), stringSerde.serializer());
    inputTopicB = testDriver.createInputTopic(MultiPartitionJoinWithTimeToleration.INPUT_TOPIC_B, stringSerde.serializer(), stringSerde.serializer());
    outputTopic = testDriver.createOutputTopic(MultiPartitionJoinWithTimeToleration.OUTPUT_TOPIC, stringSerde.deserializer(), stringSerde.deserializer());
  }

  @AfterEach
  void tearDown() {
    testDriver.close();
  }

  @ParameterizedTest
  @CsvSource({
      "0",
      "1",
      "9",
      "10",
  })
  void shouldJoin_whenSecondEventArrivesWithinTheTolerationWindow(int delaySecond) {
    assertThat(outputTopic.isEmpty()).isTrue();
    Instant now = Instant.now();
    inputTopicA.pipeInput("key", "A1", now);
    inputTopicB.pipeInput("key", "B1", now.plusSeconds(delaySecond));
    assertThat(outputTopic.isEmpty()).isFalse();
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("key", "A1,B1"));
  }

  @Test
  void shouldNotJoin_whenSecondEventArrives11SecondAfterFirstEvent() {
    assertThat(outputTopic.isEmpty()).isTrue();
    Instant now = Instant.now();
    inputTopicA.pipeInput("key", "A1", now);
    inputTopicB.pipeInput("key", "B1", now.plusSeconds(11));
    assertThat(outputTopic.isEmpty()).isTrue();
  }

  @Test
  void shouldNotJoin_whenKeysAreDifferent() {
    assertThat(outputTopic.isEmpty()).isTrue();
    Instant now = Instant.now();
    inputTopicA.pipeInput("key", "A1", now);
    inputTopicB.pipeInput("otherKey", "B1", now);
    assertThat(outputTopic.isEmpty()).isTrue();
  }

  @Test
  void shouldJoinByKey() {
    // given
    assertThat(outputTopic.isEmpty()).isTrue();
    Instant now = Instant.now();

    // when - then
    inputTopicA.pipeInput("k1", "v1-1", now);
    assertThat(outputTopic.isEmpty()).isTrue();

    // when - then
    inputTopicA.pipeInput("k2", "v2-1", now.plusSeconds(1));
    assertThat(outputTopic.isEmpty()).isTrue();

    // when
    inputTopicB.pipeInput("k1", "v1-2", now.plusSeconds(2));
    // then
    assertThat(outputTopic.isEmpty()).isFalse();
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("k1", "v1-1,v1-2"));
    assertThat(outputTopic.isEmpty()).isTrue();

    // when
    inputTopicB.pipeInput("k2", "v2-2", now.plusSeconds(3));
    // then
    assertThat(outputTopic.isEmpty()).isFalse();
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("k2", "v2-1,v2-2"));
    assertThat(outputTopic.isEmpty()).isTrue();
  }
}