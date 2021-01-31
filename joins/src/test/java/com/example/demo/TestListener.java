package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.*;

import static com.example.demo.MultiPartitionJoinWithTimeToleration.OUTPUT_TOPIC;

@Slf4j
@Component
public class TestListener implements ConsumerSeekAware {
  private boolean ready = false;
  private List<String> receivedEvents = new ArrayList<>();

  @KafkaListener(topics = OUTPUT_TOPIC)
  private void consumer(String record) {
    log.info("received {}", record);
    receivedEvents.add(record);
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    ready = false;
  }

  @Override
  public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekAware.ConsumerSeekCallback callback) {
    ready = true;
  }

  public boolean isReady() {
    return ready;
  }

  public Optional<String> getRecord(String record){
    return receivedEvents.stream().filter(s -> receivedEvents.contains(record)).findFirst();
  }
}
