package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConstant;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;


public class KafkaUtil {
    public static KafkaSource<String> getKafkaSource(String groupId, String topic) {
        return KafkaSource.<String>builder()
            .setBootstrapServers(GmallConstant.KAFKA_BROKERS)
            .setGroupId(groupId)
            .setTopics(topic)
            .setStartingOffsets(OffsetsInitializer.latest()) // 没有状态或者状态的没有偏移量从最新的消费.如果是从状态恢复, 从状态中记录的位置开始消费
            .setValueOnlyDeserializer(new DeserializationSchema<String>() {  // 自定义反序列化
                
                // 反序列化: 把字节数组转成字符串
                @Override
                public String deserialize(byte[] message) throws IOException {
                    if (message != null) {
                        return new String(message, StandardCharsets.UTF_8);
                    }
                    return null;  // 如果返回的是 null,则这条数据直接忽略
                }
                
                // 是否结束流
                // 无界流永远返回 false
                @Override
                public boolean isEndOfStream(String nextElement) {
                    return false;
                }
                
                // 返回得到流中数据的类型
                @Override
                public TypeInformation<String> getProducedType() {
                    //                    return Types.STRING;  // 专用: 必须一些常见的内置类型
                    //                    return TypeInformation.of(String.class);  // 普通的不带泛型的类型的写法
                    return TypeInformation.of(new TypeHint<String>() {}); // 最通用: 适用于所有类型
                }
            })
            .build();
        
    }

    /**
     * 默认的 setTopicSelector
     * 默认的  setValueSerializationSchema
     * @param topic
     * @return
     */
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


    /**
     * 自定义的 setTopicSelector
     * 自定义的  setValueSerializationSchema
     * @return
     */

    public static Sink<Tuple2<JSONObject, TableProcess>> getKafkaSink() {
        return KafkaSink.<Tuple2<JSONObject, TableProcess>>builder()
                .setBootstrapServers(GmallConstant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.<Tuple2<JSONObject, TableProcess>>builder()
                        // topic 可以从数据中提取
                        // 流中每来一条数据,则这个方法(apply)执行一次,返回值表示这个条数据要去的 topic
                        .setTopicSelector( t -> t.f1.getSinkTable())
                        .setValueSerializationSchema(new SerializationSchema<Tuple2<JSONObject, TableProcess>>() {
                            // 字节数组: 把要写到 kafka 的 value 变成字节数组
                            @Override
                            public byte[] serialize(Tuple2<JSONObject, TableProcess> t) {
                                return t.f0.toJSONString().getBytes(StandardCharsets.UTF_8);
                            }
                        }) // 设置 value 的序列化器
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE) // 设置一致性级别
                .setTransactionalIdPrefix("atguigu: " + new Random().nextLong()) // 如果是严格一次,则必须设置事务 id 的前缀
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "") // 生产者的事务超时时间不能大于服务器运行的最大值(15 分钟)
                .build();
    }
}

