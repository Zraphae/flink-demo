package cn.com.my.common.schemas;

import cn.com.my.common.model.OGGMessage;
import cn.com.my.common.utils.GsonUtil;
import cn.com.my.common.utils.HBaseUtils;
import com.google.gson.JsonObject;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

@Slf4j
@NoArgsConstructor
@AllArgsConstructor
public class OGGMessageSchema implements KafkaDeserializationSchema<OGGMessage>, KafkaSerializationSchema<String> {

    private String topicName;
    private String primaryKey;

    @Override
    public boolean isEndOfStream(OGGMessage nextElement) {
        return false;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
        byte[] keyBytes = null;
        if (!StringUtils.isBlank(primaryKey)) {
            JsonObject jsonObject = GsonUtil.parse2JsonObj(element);
            String primaryValues = HBaseUtils.getPrimaryValues(primaryKey, jsonObject);
            keyBytes = primaryValues.getBytes(StandardCharsets.UTF_8);
        }
        byte[] valueBytes = element.getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(topicName, keyBytes, valueBytes);
    }

    @Override
    public OGGMessage deserialize(ConsumerRecord<byte[], byte[]> record) {

        byte[] value = record.value();
        byte[] key = record.key();
        OGGMessage oggMessage = new OGGMessage();
        if (!Objects.isNull(value)) {
            String jsonStr = new String(value, StandardCharsets.UTF_8);
            if (log.isDebugEnabled()) {
                log.debug("=====>json: {}", jsonStr);
            }
            oggMessage = GsonUtil.fromJson(jsonStr, OGGMessage.class);
            oggMessage.setOffset(record.offset());
            oggMessage.setTopicName(record.topic());
            oggMessage.setPartition(record.partition());
        }
        if (!Objects.isNull(key)) {
            String keyStr = new String(key, StandardCharsets.UTF_8);
            if (log.isDebugEnabled()) {
                log.debug("=====>key: {}", keyStr);
            }
            oggMessage.setKey(keyStr);
        }

        return oggMessage;
    }

    @Override
    public TypeInformation<OGGMessage> getProducedType() {
        return TypeExtractor.getForClass(OGGMessage.class);
    }


}
