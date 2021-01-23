import com.example.demo.wordcount._4tableToStream.WordCountGroupByKeyDslTableToStream;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class WordCountGroupByKeyDslTableToStreamTest {
  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, Long> outputTopic;
  private KeyValueStore<String, Long> store;
  private final Serde<String> stringSerde = new Serdes.StringSerde();
  private final Serde<Long> longSerde = new Serdes.LongSerde();

  @BeforeEach
  void setUp() {
    // set up properties for unit test
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "dummy");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy");
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());

    // set up TopologyTestDriver
    testDriver = new TopologyTestDriver(WordCountGroupByKeyDslTableToStream.getTopology(), properties);

    // setup test topics
    inputTopic = testDriver.createInputTopic(WordCountGroupByKeyDslTableToStream.INPUT_TOPIC, stringSerde.serializer(), stringSerde.serializer());
    outputTopic = testDriver.createOutputTopic(WordCountGroupByKeyDslTableToStream.OUTPUT_TOPIC, stringSerde.deserializer(), longSerde.deserializer());

    // setup test state store
    store = testDriver.getKeyValueStore(WordCountGroupByKeyDslTableToStream.STORE_NAME);
  }

  @AfterEach
  void tearDown() {
    testDriver.close();
  }

  @Test
  void countWordsInOutputTopic() {
    // given
    assertThat(outputTopic.isEmpty()).isTrue();

    // when
    inputTopic.pipeInput("hello world");
    inputTopic.pipeInput("hello joseph");

    // then
    assertThat(outputTopic.isEmpty()).isFalse();
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("hello", 1L));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("world", 1L));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("hello", 2L));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("joseph", 1L));
    assertThat(outputTopic.isEmpty()).isTrue();
  }

  @Test
  void countWordsInStateStore() {
    // given
    assertThat(outputTopic.isEmpty()).isTrue();

    // when
    inputTopic.pipeInput("hello world");
    inputTopic.pipeInput("hello joseph");

    // then
    assertThat(store.get("hello")).isEqualTo(2L);
    assertThat(store.get("world")).isEqualTo(1L);
    assertThat(store.get("joseph")).isEqualTo(1L);
  }
}
