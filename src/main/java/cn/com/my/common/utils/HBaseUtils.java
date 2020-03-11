package cn.com.my.common.utils;

import cn.com.my.common.constant.OGGOpType;
import cn.com.my.common.model.OGGMessage;
import com.google.common.base.Joiner;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;

public class HBaseUtils {

    public static final String DELETE_FLAG = "DELETE_FLAG";

    public static String getHBaseRowKey(OGGMessage oggMessage, String primaryKeyName) {

        JsonObject jsonObject;
        if(StringUtils.equals(OGGOpType.DELETE.getValue(), oggMessage.getOpType())) {
            jsonObject = GsonUtil.parse2JsonObj(oggMessage.getBefore().toString());
        }else {
            jsonObject = GsonUtil.parse2JsonObj(oggMessage.getAfter().toString());
        }
        String primaryValues = getPrimaryValues(primaryKeyName, jsonObject);
        return primaryValues;
    }


    public static String getPrimaryValues(String primaryKeyName, JsonObject jsonObject){

        String[] primaryKeys = primaryKeyName.split(",");
        String[] keyValues = new String[primaryKeys.length];
        for(int index=0; index<primaryKeys.length; index++){
            String keyValue = jsonObject.get(primaryKeys[index]).getAsString();
            keyValues[index] = keyValue;
        }
        return Joiner.on("_").join(keyValues);
    }

}
