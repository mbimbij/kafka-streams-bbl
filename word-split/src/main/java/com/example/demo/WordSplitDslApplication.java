package com.example.demo;

import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

public class WordSplitDslApplication {

  public static final String INPUT_TOPIC = "word-split-input";
  public static final String OUTPUT_TOPIC = "word-split-output";

  @SneakyThrows
  public static void main(String[] args) {
    // initialisation des configs/props
    Properties properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "line-split-dsl");
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
    builder.<String,String>stream(INPUT_TOPIC)
        .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
        .to(OUTPUT_TOPIC);
    return builder.build();
  }

}
