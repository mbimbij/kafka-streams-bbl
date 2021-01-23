package com.example.demo.wordcount._2groupbykey;

import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Grouped;

import java.util.Arrays;
import java.util.Properties;

public class WordCountGroupByKeyDslErrorApplication {

  public static final String INPUT_TOPIC = "wordcount-input";
  public static final String OUTPUT_TOPIC = "wordcount-output";

  @SneakyThrows
  public static void main(String[] args) {
    // initialisation des configs/props
    Properties properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-dsl-groupByKey");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    // définition de la "topologie" -> le stream processing que l'on va appliquer
    Topology topology = getTopology();

    // lancement de l'application kafka streams
    KafkaStreams streams = new KafkaStreams(topology, properties);
    streams.start();
  }

  public static Topology getTopology() {
    StreamsBuilder builder = new StreamsBuilder();
    builder.<String, String>stream(INPUT_TOPIC)
        .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
        .map((key, value) -> new KeyValue<>(value, 1))
        .groupByKey()
        .count();
    return builder.build();
  }

}
