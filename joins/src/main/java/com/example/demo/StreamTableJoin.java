package com.example.demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import java.time.Duration;
import java.util.Properties;

public class StreamTableJoin implements ApplicationRunner {

  public static final String INPUT_TOPIC_A = "join_input_a";
  public static final String INPUT_TOPIC_B = "join_input_b";
  public static final String OUTPUT_TOPIC = "join-output";

  @Override
  public void run(ApplicationArguments args) {
    // initialisation des configs/props
    Properties properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "sliding-window-sum");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    // définition de la "topologie" -> le stream processing que l'on va appliquer
    Topology topology = getTopology();

    // lancement de l'application kafka streams et création du queryable state store
    KafkaStreams streams = new KafkaStreams(topology, properties);
    streams.start();
  }

  public static Topology getTopology() {
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> inputAStream = builder.stream(INPUT_TOPIC_A);
    KStream<String, String> inputBStream = builder.stream(INPUT_TOPIC_B);
    inputAStream
        .join(
            inputBStream.toTable(),
            (value1, value2) -> String.join(",", value1, value2))
        .to(OUTPUT_TOPIC);
    return builder.build();
  }
}
