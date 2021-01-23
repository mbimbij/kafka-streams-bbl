package com.example.demo.wordcount._3interactiveQueries;

import io.vavr.Tuple2;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

@SpringBootApplication
@RestController
public class WordCountGroupByKeyDslWithInteractiveQueries implements ApplicationRunner {

  public static final String INPUT_TOPIC = "wordcount-input";
  public static final String OUTPUT_TOPIC = "wordcount-output";
  private static String STORE_NAME = "wordcount-store";
  private ReadOnlyKeyValueStore<String, Long> queryableStateStore;

  public static void main(String[] args) {
    SpringApplication.run(WordCountGroupByKeyDslWithInteractiveQueries.class, args);
  }

  @GetMapping("/count")
  public Map<String, Long> countAll() {
    Map<String, Long> retval = new HashMap<>();
    queryableStateStore.all().forEachRemaining(entry -> retval.put(entry.key, entry.value));
    return retval;
  }

  @GetMapping("/count/{word}")
  public Map<String, Long> countByWord(@PathVariable("word") String word) {
    return Map.of(word, Optional.ofNullable(queryableStateStore.get(word)).orElse(0L));
  }

  @Override
  public void run(ApplicationArguments args) {
    // initialisation des configs/props
    Properties properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-dsl-groupByKey");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    // définition de la "topologie" -> le stream processing que l'on va appliquer
    Topology topology = getTopology();

    // lancement de l'application kafka streams et création du queryable state store
    KafkaStreams streams = new KafkaStreams(topology, properties);
    streams.start();
    queryableStateStore = streams.store(StoreQueryParameters.fromNameAndType(STORE_NAME, QueryableStoreTypes.keyValueStore()));
  }

  public static Topology getTopology() {
    StreamsBuilder builder = new StreamsBuilder();
    builder.<String, String>stream(INPUT_TOPIC)
        .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
        .map((key, value) -> new KeyValue<>(value, 1))
        .groupByKey(Grouped.valueSerde(Serdes.Integer()))
        .count(Materialized.as(STORE_NAME));
    return builder.build();
  }
}
