package cn.com.my.common.model;


import cn.com.my.common.constant.OGGOpType;
import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OGGMessage {

    private long offset;
    private String topicName;
    private int partition;
    private String key;

    @SerializedName("op_type")
    private String opType;
    @SerializedName("op_ts")
    private String opTs;
    @SerializedName("current_ts")
    private String currentTs;

    private String table;
    private String pos;

    private Map<String, String> before;
    private Map<String, String> after;


    public Map<String, String> getKeyValues() {
        if (StringUtils.equals(OGGOpType.D.getValue(), this.getOpType())) {
            return this.before;
        }
        return this.after;
    }

}
