package com.example.demo.windowing.sliding;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
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


public class SlidingWindowSumTest {
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
    testDriver = new TopologyTestDriver(SlidingWindowSum.getTopology(), properties);

    // setup test topics
    inputTopic = testDriver.createInputTopic(SlidingWindowSum.INPUT_TOPIC, stringSerde.serializer(), floatSerde.serializer());
    outputTopic = testDriver.createOutputTopic(SlidingWindowSum.OUTPUT_TOPIC, WindowedSerdes.timeWindowedSerdeFrom(String.class, SlidingWindowSum.WINDOW_SIZE_MILLIS).deserializer(), floatSerde.deserializer());

    // setup test state store
    store = testDriver.getWindowStore(SlidingWindowSum.STORE_NAME);
  }

  @AfterEach
  void tearDown() {
    testDriver.close();
  }

  @Test
  void countWordsInOutputTopic() {
    // given

    // when
//    DateTimeFormatter formatter =
//        DateTimeFormatter.ofLocalizedDateTime( FormatStyle.SHORT )
//            .withLocale( Locale.FRANCE )
//            .withZone( ZoneId.systemDefault() );

    DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.systemDefault());

    Instant now = Instant.now();
    System.out.println("coucou: "+formatter.format(now));

    inputTopic.pipeInput("key", 1f, now);
    System.out.println("#########################################################");
    System.out.println("après 1e évènement");
    System.out.println("#########################################################");
    printStore();
    printOutputTopic();

    inputTopic.pipeInput("key", 1f, now.plusSeconds(1));
    System.out.println("#########################################################");
    System.out.println("après 2e évènement");
    System.out.println("#########################################################");
    printStore();
    printOutputTopic();

    inputTopic.pipeInput("key", 1f, now.plusSeconds(2));
    System.out.println("#########################################################");
    System.out.println("après 3e évènement");
    System.out.println("#########################################################");
    printStore();
    printOutputTopic();

//    inputTopic.pipeInput("key", 1f, now.plusSeconds(3));
//    System.out.println("après 4e évènement");
//    inputTopic.pipeInput("key", 1f, now.plusSeconds(4));
//    System.out.println("après 5e évènement");
//    inputTopic.pipeInput("key", 1f, now.plusSeconds(5));
//
//    System.out.println("après 6e évènement");
//    inputTopic.pipeInput("key", 1f, now.plusSeconds(10));
//    System.out.println("après 7e évènement");
//    inputTopic.pipeInput("key", 1f, now.plusSeconds(11));
//    System.out.println("après 8e évènement");
//    inputTopic.pipeInput("key", 1f, now.plusSeconds(12));
//    System.out.println("après 9e évènement");
    // then
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
