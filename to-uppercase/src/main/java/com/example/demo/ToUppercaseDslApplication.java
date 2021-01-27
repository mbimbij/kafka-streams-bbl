package com.example.demo;

import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Locale;
import java.util.Properties;

public class ToUppercaseDslApplication {

  public static final String INPUT_TOPIC = "to-uppercase-input";
  public static final String OUTPUT_TOPIC = "to-uppercase-output";

  @SneakyThrows
  public static void main(String[] args) {
    // initialisation des configs/props
    Properties properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "to-uppercase-dsl");
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
        .mapValues(value -> value.toUpperCase(Locale.FRANCE))
        .to(OUTPUT_TOPIC);
    return builder.build();
  }

}
