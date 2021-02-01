package com.example.demo.windowing.sliding;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;


public class HoppingWindowSumTest {
  private TopologyTestDriver testDriver;
  private TestInputTopic<String, Float> inputTopic;
  private TestOutputTopic<Windowed<String>, Float> outputTopic;
  private WindowStore<String, Float> store;
  private final Serde<String> stringSerde = new Serdes.StringSerde();
  private final Serde<Float> floatSerde = new Serdes.FloatSerde();

  @BeforeEach
  void setUp() {
    // set up properties for unit test
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "dummy");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy");
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, floatSerde.getClass().getName());

    // set up TopologyTestDriver
    HoppingWindowSumSuppressed streamApplication = new HoppingWindowSumSuppressed("dummy", 3000, 3);
    testDriver = new TopologyTestDriver(streamApplication.getTopology(), properties);

    // setup test topics
    inputTopic = testDriver.createInputTopic(HoppingWindowSum.INPUT_TOPIC, stringSerde.serializer(), floatSerde.serializer());
    outputTopic = testDriver.createOutputTopic(HoppingWindowSum.OUTPUT_TOPIC, WindowedSerdes.timeWindowedSerdeFrom(String.class, streamApplication.getWindowSizeMillis()).deserializer(), floatSerde.deserializer());

    // setup test state store
    store = testDriver.getWindowStore(HoppingWindowSum.STORE_NAME);
  }

  @AfterEach
  void tearDown() {
    testDriver.close();
  }

  @Test
  void hoppingWindowExplorationTest() {
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd.MM.yyyy'T'HH:mm:ss.SSSXXX");

    Instant start = Instant.now();
    System.out.println("start: " + dtf.format(ZonedDateTime.ofInstant(start, ZoneId.systemDefault())));

    inputTopic.pipeInput("key", 1f, start);
    System.out.println("#########################################################");
    System.out.println("après 1e évènement - " + dtf.format(ZonedDateTime.ofInstant(start, ZoneId.systemDefault())));
    System.out.println("#########################################################");
    printStore();
    printOutputTopic();

    inputTopic.pipeInput("key", 1f, start.plusMillis(1500));
    System.out.println("#########################################################");
    System.out.println("après 2e évènement - " + dtf.format(ZonedDateTime.ofInstant(start, ZoneId.systemDefault())));
    System.out.println("#########################################################");
    printStore();
    printOutputTopic();

    inputTopic.pipeInput("key", 1f, start.plusMillis(2000));
    System.out.println("#########################################################");
    System.out.println("après 3e évènement - " + dtf.format(ZonedDateTime.ofInstant(start, ZoneId.systemDefault())));
    System.out.println("#########################################################");
    printStore();
    printOutputTopic();
  }

  @Test
  void whenEventsArePublishedAfterGracePeriod_thenTheyAreNotIncludedInWindow() {
    // given
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd.MM.yyyy'T'HH:mm:ss.SSSXXX");
    Instant start = Instant.now();
    System.out.println("start: " + dtf.format(ZonedDateTime.ofInstant(start, ZoneId.systemDefault())));
    assertThat(outputTopic.isEmpty()).isTrue();

    // when - then
    inputTopic.pipeInput("key", 1f, start);
//    assertThat(outputTopic.isEmpty()).isFalse();
//    outputTopic.readKeyValuesToList();
//    assertThat(outputTopic.isEmpty()).isTrue();
    System.out.println("#########################################################");
    System.out.println("après 1e évènement - " + dtf.format(ZonedDateTime.ofInstant(start, ZoneId.systemDefault())));
    System.out.println("#########################################################");
    printStore();
    printOutputTopic();

    // when - then
    testDriver.advanceWallClockTime(Duration.ofMinutes(12));
    inputTopic.pipeInput("key", 1f, start);
//    assertThat(outputTopic.isEmpty()).isTrue();
    System.out.println("#########################################################");
    System.out.println("après 2e évènement - " + dtf.format(ZonedDateTime.ofInstant(start, ZoneId.systemDefault())));
    System.out.println("#########################################################");
    printStore();
    printOutputTopic();
  }

  private void printOutputTopic() {
    System.out.println("coucou output topic");
    outputTopic.readKeyValuesToList().stream()
        .map(this::storedWindowToString)
        .forEach(System.out::println);
  }

  private void printStore() {
    KeyValueIterator<Windowed<String>, Float> all = store.all();
    System.out.println("coucou store");
    while (all.hasNext()) {
      KeyValue<Windowed<String>, Float> next = all.next();
      System.out.println(storedWindowToString(next));
    }
  }

  String storedWindowToString(KeyValue<Windowed<String>, Float> keyValue) {
    String pattern = "dd.MM.yyyy'T'HH:mm:ss.SSSXXX";
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern(pattern);
    return MessageFormat.format("[{0}; {1}] -> {2}",
        dtf.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(keyValue.key.window().start()), ZoneId.systemDefault())),
        dtf.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(keyValue.key.window().end()), ZoneId.systemDefault())),
        keyValue.value);
  }
}
