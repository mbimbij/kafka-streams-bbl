package com.example.demo.windowing.sliding;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.text.MessageFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;


public class SessionWindowSumTest {
  private TopologyTestDriver testDriver;
  private TestInputTopic<String, Float> inputTopic;
  private TestOutputTopic<Windowed<String>, Float> outputTopic;
  private SessionStore<String, Float> store;
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
    testDriver = new TopologyTestDriver(SessionWindowSum.getTopology(), properties);

    // setup test topics
    inputTopic = testDriver.createInputTopic(SessionWindowSum.INPUT_TOPIC, stringSerde.serializer(), floatSerde.serializer());
    outputTopic = testDriver.createOutputTopic(SessionWindowSum.OUTPUT_TOPIC, WindowedSerdes.sessionWindowedSerdeFrom(String.class).deserializer(), floatSerde.deserializer());

    // setup test state store
    store = testDriver.getSessionStore(SessionWindowSum.STORE_NAME);
  }

  @AfterEach
  void tearDown() {
    testDriver.close();
  }

  @Test
  void sessionWindowExplorationTest() {
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd.MM.yyyy'T'HH:mm:ss.SSSXXX");

    Instant start = Instant.now();
    System.out.println("coucou: "+dtf.format(ZonedDateTime.ofInstant(start, ZoneId.systemDefault())));

    inputTopic.pipeInput("key", 1f, start);
    System.out.println("#########################################################");
    System.out.println("après 1e évènement");
    System.out.println("#########################################################");
    printStore();
    printOutputTopic();

    inputTopic.pipeInput("key", 1f, start.plusMillis(4000));
    System.out.println("#########################################################");
    System.out.println("après 2e évènement");
    System.out.println("#########################################################");
    printStore();
    printOutputTopic();

    inputTopic.pipeInput("key", 1f, start.plusMillis(2000));
    System.out.println("#########################################################");
    System.out.println("après 3e évènement");
    System.out.println("#########################################################");
    printStore();
    printOutputTopic();

    inputTopic.pipeInput("key", 1f, start.plusMillis(10000));
    System.out.println("#########################################################");
    System.out.println("après 4e évènement");
    System.out.println("#########################################################");
    printStore();
    printOutputTopic();
    inputTopic.pipeInput("key", 1f, start.plusMillis(11000));
    System.out.println("#########################################################");
    System.out.println("après 5e évènement");
    System.out.println("#########################################################");
    printStore();
    printOutputTopic();
    inputTopic.pipeInput("key", 1f, start.plusMillis(10500));
    System.out.println("#########################################################");
    System.out.println("après 6e évènement");
    System.out.println("#########################################################");
    printStore();
    printOutputTopic();

//    System.out.println("########### coucou début ###########");
//    System.out.println("coucou: "+dtf.format(ZonedDateTime.ofInstant(start, ZoneId.systemDefault())));
//    try (WindowStoreIterator<Float> iterator = store.fetch("key", start.minusMillis(400), start.minusMillis(200))) {
//      iterator.forEachRemaining(item -> System.out.printf("[%s, %f]%n",
//          dtf.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(item.key), ZoneId.systemDefault())),
//          item.value
//      ));
//    }
//    System.out.println("########### coucou fin ###########");

  }

  private void printOutputTopic() {
    System.out.println("coucou output topic");
    outputTopic.readKeyValuesToList().stream()
        .map(this::storedWindowToString)
        .forEach(System.out::println);
  }

  private void printStore() {
    KeyValueIterator<Windowed<String>, Float> all = store.fetch("key");
    System.out.println("coucou store");
    while (all.hasNext()){
      KeyValue<Windowed<String>, Float> next = all.next();
      System.out.println(storedWindowToString(next));
    }
  }

  String storedWindowToString(KeyValue<Windowed<String>, Float> keyValue){
    String pattern = "dd.MM.yyyy'T'HH:mm:ss.SSSXXX";
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern(pattern);
    return MessageFormat.format("[{0}; {1}] -> {2}",
        dtf.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(keyValue.key.window().start()), ZoneId.systemDefault())),
        dtf.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(keyValue.key.window().end()), ZoneId.systemDefault())),
        keyValue.value);
  }
}
