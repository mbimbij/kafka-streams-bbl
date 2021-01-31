package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.*;

import static com.example.demo.MultiPartitionJoinWithTimeToleration.OUTPUT_TOPIC;

@Slf4j
@Component
public class TestListenerIT implements ConsumerSeekAware {
  private boolean ready = false;
  private Map<String, List<String>> receivedRecords = new HashMap<>();

  @KafkaListener(topics = OUTPUT_TOPIC)
  private void consumer(ConsumerRecord<String, String> record) {
    log.info("received {}", record);
    String recordKey = record.key();
    receivedRecords.putIfAbsent(recordKey, new ArrayList<>());
    receivedRecords.get(recordKey).add(record.value());
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    ready = false;
  }

  @Override
  public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekAware.ConsumerSeekCallback callback) {
    ready = true;
  }

  public void clear() {
    receivedRecords.clear();
  }

  public boolean isReady() {
    return ready;
  }

  public List<String> getByKey(String key){
    receivedRecords.putIfAbsent(key, new ArrayList<>());
    return receivedRecords.get(key);
  }
}
