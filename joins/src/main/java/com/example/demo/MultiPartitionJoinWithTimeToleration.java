package com.example.demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import java.time.Duration;
import java.util.Properties;

public class MultiPartitionJoinWithTimeToleration implements ApplicationRunner {

  public static final String INPUT_TOPIC_A = "join_input_a";
  public static final String INPUT_TOPIC_B = "join_input_b";
  public static final String OUTPUT_TOPIC = "join-output";
  private final String bootstrapServers;

  public MultiPartitionJoinWithTimeToleration(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  @Override
  public void run(ApplicationArguments args) {
    // initialisation des configs/props
    Properties properties = getProperties();

    // définition de la "topologie" -> le stream processing que l'on va appliquer
    Topology topology = getTopology();

    // lancement de l'application kafka streams et création du queryable state store
    KafkaStreams streams = new KafkaStreams(topology, properties);
    streams.start();
  }

  public Properties getProperties() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "sliding-window-sum");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    return properties;
  }

  public static Topology getTopology() {
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> inputAStream = builder.stream(INPUT_TOPIC_A);
    KStream<String, String> inputBStream = builder.stream(INPUT_TOPIC_B);
    inputAStream
        .join(
            inputBStream,
            (value1, value2) -> String.join(",", value1, value2),
            JoinWindows.of(Duration.ofSeconds(10)))
        .to(OUTPUT_TOPIC);
    return builder.build();
  }
}
