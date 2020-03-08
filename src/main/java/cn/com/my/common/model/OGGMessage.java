package cn.com.my.common.model;


import com.google.gson.annotations.SerializedName;
import lombok.Builder;
import lombok.Data;


@Data
@Builder
public class OGGMessage {

    private String table;
    private String pos;
    private String data;
    @SerializedName("op_type")
    private String opType;
    @SerializedName("op_ts")
    private String opTs;
}
