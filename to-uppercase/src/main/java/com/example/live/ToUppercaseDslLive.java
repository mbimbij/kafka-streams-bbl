package com.example.live;

import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Locale;
import java.util.Properties;

public class ToUppercaseDslLive {

  public static final String INPUT_TOPIC = "to-uppercase-input";
  public static final String OUTPUT_TOPIC = "to-uppercase-output";

  @SneakyThrows
  public static void main(String[] args) {
  }
}
