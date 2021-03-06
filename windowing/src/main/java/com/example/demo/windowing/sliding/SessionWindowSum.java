package com.example.demo.windowing.sliding;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.SessionStore;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import java.time.Duration;
import java.util.Properties;

public class SessionWindowSum implements ApplicationRunner {

  public static final String INPUT_TOPIC = "session-window-sum-input";
  public static final String OUTPUT_TOPIC = "session-window-sum-output";
  public static final String STORE_NAME = "session-window-sum-store";
  public static final int WINDOW_SIZE_MILLIS = 3000;
  private ReadOnlyWindowStore<String, Float> queryableStateStore;

  @Override
  public void run(ApplicationArguments args) {
    // initialisation des configs/props
    Properties properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "session-window-sum");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Float().getClass());

    // définition de la "topologie" -> le stream processing que l'on va appliquer
    Topology topology = getTopology();

    // lancement de l'application kafka streams et création du queryable state store
    KafkaStreams streams = new KafkaStreams(topology, properties);
    streams.start();
    queryableStateStore = streams.store(StoreQueryParameters.fromNameAndType(STORE_NAME, QueryableStoreTypes.windowStore()));
  }

  public static Topology getTopology() {
    StreamsBuilder builder = new StreamsBuilder();
    builder.<String, Float>stream(INPUT_TOPIC)
        .groupByKey()
        .windowedBy(SessionWindows.with(Duration.ofMillis(WINDOW_SIZE_MILLIS)))
//        .aggregate(
//            () -> 0f,
//            (aggKey, newValue, aggValue) -> aggValue + newValue,
//            (aggKey, leftAggValue, rightAggValue) -> leftAggValue + rightAggValue,
//            Materialized.as(STORE_NAME)
//        )
        .reduce(Float::sum, Materialized.as(STORE_NAME))
        .toStream()
        .to(OUTPUT_TOPIC, Produced.keySerde(WindowedSerdes.sessionWindowedSerdeFrom(String.class)));
    return builder.build();
  }
}
