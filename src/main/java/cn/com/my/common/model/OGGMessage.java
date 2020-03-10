package cn.com.my.common.model;


import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OGGMessage {

    private String table;
    private String pos;
    private Object data;
    @SerializedName("op_type")
    private String opType;
    @SerializedName("op_ts")
    private String opTs;

    private long offset;
    private String topic;
    private int partition;
    private String key;
}
