package com.example.demo;

import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ToUppercaseDslApplication {

  @SneakyThrows
  public static void main(String[] args) {
    // initialisation des configs/props
    Properties properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "to-uppercase");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    // définition de la "topologie" -> le stream processing que l'on va appliquer
    StreamsBuilder builder = new StreamsBuilder();
    builder.<String,String>stream("to-uppercase-input")
        .mapValues(value -> value.toUpperCase(Locale.FRANCE))
        .to("to-uppercase-output");

    // lancement de l'application kafka streams
    KafkaStreams streams = new KafkaStreams(builder.build(), properties);
    streams.start();
  }

}
