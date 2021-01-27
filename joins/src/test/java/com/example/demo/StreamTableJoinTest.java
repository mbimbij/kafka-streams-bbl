package com.example.demo;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class StreamTableJoinTest {
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
    testDriver = new TopologyTestDriver(StreamTableJoin.getTopology(), properties);

    // setup test topics
    inputTopicA = testDriver.createInputTopic(StreamTableJoin.INPUT_TOPIC_A, stringSerde.serializer(), stringSerde.serializer());
    inputTopicB = testDriver.createInputTopic(StreamTableJoin.INPUT_TOPIC_B, stringSerde.serializer(), stringSerde.serializer());
    outputTopic = testDriver.createOutputTopic(StreamTableJoin.OUTPUT_TOPIC, stringSerde.deserializer(), stringSerde.deserializer());
  }

  @AfterEach
  void tearDown() {
    testDriver.close();
  }

  @Test
  void exploreJoins() {
    assertThat(outputTopic.isEmpty()).isTrue();
    inputTopicB.pipeInput("key","B1");
    inputTopicA.pipeInput("key","A1");
    assertThat(outputTopic.isEmpty()).isFalse();
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("key","A1,B1"));
  }

}