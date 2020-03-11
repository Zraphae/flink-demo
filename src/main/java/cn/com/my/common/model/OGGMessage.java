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

    private Object before;
    private Object after;


}
