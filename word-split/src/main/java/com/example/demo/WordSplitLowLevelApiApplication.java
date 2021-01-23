package com.example.demo;

import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

public class WordSplitLowLevelApiApplication {

  public static final String INPUT_TOPIC = "word-split-input";
  public static final String OUTPUT_TOPIC = "word-split-output";

  @SneakyThrows
  public static void main(String[] args) {
    // initialisation des configs/props
    Properties properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-split-low-level-api");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    // définition de la "topologie" -> le stream processing que l'on va appliquer
    Topology topology = getTopology();

    // lancement de l'application kafka streams
    KafkaStreams streams = new KafkaStreams(topology, properties);
    streams.start();
  }

  public static Topology getTopology() {
    Topology topology = new Topology();
    topology.addSource("source", INPUT_TOPIC);
    topology.addProcessor("processor", WordSplitProcessor::new, "source");
    topology.addSink("sink", OUTPUT_TOPIC, "processor");
    return topology;
  }

  private static class WordSplitProcessor implements Processor<String, String, String, String> {
    private ProcessorContext<String, String> context;

    @Override
    public void init(ProcessorContext<String, String> context) {
      this.context = context;
    }

    @Override
    public void process(Record<String, String> record) {
      Arrays.stream(record.value().split("\\W+"))
          .forEach(split -> {
            Record<String, String> processedRecord = new Record<>(record.key(),
                split,
                ZonedDateTime.now().toInstant().toEpochMilli());
            context.forward(processedRecord);
          });
    }
  }
}
