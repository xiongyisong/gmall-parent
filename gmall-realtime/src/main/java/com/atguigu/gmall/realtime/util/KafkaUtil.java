package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.common.GmallConstant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Random;


public class KafkaUtil {
    public static KafkaSource<String> getKafkaSource(String groupId, String topic){

        return  KafkaSource.<String>builder()
                .setBootstrapServers(GmallConstant.KAFKA_BROKERS)
                .setGroupId(groupId)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    public static Sink<String> getKafkaSink(String topic) {
       return  KafkaSink.<String>builder()
                .setBootstrapServers(GmallConstant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
               .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
               .setProperty("transaction.timeout.ms",15*60*1000+"")
               .setTransactionalIdPrefix(topic+new Random().nextLong())
               .build();

    }
}
