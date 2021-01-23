package com.example.demo;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class WordSplitDslApplicationTest {
  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
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
    testDriver = new TopologyTestDriver(WordSplitDslApplication.getTopology(), properties);

    // setup test topics
    inputTopic = testDriver.createInputTopic(WordSplitDslApplication.INPUT_TOPIC, stringSerde.serializer(), stringSerde.serializer());
    outputTopic = testDriver.createOutputTopic(WordSplitDslApplication.OUTPUT_TOPIC, stringSerde.deserializer(), stringSerde.deserializer());
  }

  @AfterEach
  void tearDown() {
    testDriver.close();
  }

  @Test
  void shouldSplitOnSpace() {
    assertThat(outputTopic.isEmpty()).isTrue();
    inputTopic.pipeInput("hello world");
    assertThat(outputTopic.isEmpty()).isFalse();
    assertThat(outputTopic.readValue()).isEqualTo("hello");
    assertThat(outputTopic.isEmpty()).isFalse();
    assertThat(outputTopic.readValue()).isEqualTo("world");
    assertThat(outputTopic.isEmpty()).isTrue();
  }

  @Test
  void shouldSplitOnDot() {
    assertThat(outputTopic.isEmpty()).isTrue();
    inputTopic.pipeInput("hello.world");
    assertThat(outputTopic.isEmpty()).isFalse();
    assertThat(outputTopic.readValue()).isEqualTo("hello");
    assertThat(outputTopic.isEmpty()).isFalse();
    assertThat(outputTopic.readValue()).isEqualTo("world");
    assertThat(outputTopic.isEmpty()).isTrue();
  }
}