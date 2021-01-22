package com.example.demo;

import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Properties;

public class ToUppercaseLowLevelApiApplication {

  @SneakyThrows
  public static void main(String[] args) {
    // initialisation des configs/props
    Properties properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "to-uppercase-low-level-api-processor");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    // définition de la "topologie" -> le stream processing que l'on va appliquer
    Topology topology = getTopology();

    // lancement de l'application kafka streams
    KafkaStreams streams = new KafkaStreams(topology, properties);
    streams.start();
  }

  private static Topology getTopology() {
    Topology topology = new Topology();
    topology.addSource("source", "to-uppercase-input");
    topology.addProcessor("processor", ToUppercaseProcessor::new, "source");
    topology.addSink("to-uppercase-input-sink", "to-uppercase-output","processor");
    return topology;
  }

  private static class ToUppercaseProcessor implements Processor<String, String, String, String> {
    private ProcessorContext<String, String> context;

    @Override
    public void init(ProcessorContext<String, String> context) {
      this.context = context;
    }

    @Override
    public void process(Record<String, String> record) {
      record.withValue(record.value().toUpperCase(Locale.FRANCE));
      Record<String, String> processedRecord = new Record<>(record.key(),
          record.value().toUpperCase(Locale.FRANCE),
          ZonedDateTime.now().toInstant().toEpochMilli());
      context.forward(processedRecord);
    }
  }
}
