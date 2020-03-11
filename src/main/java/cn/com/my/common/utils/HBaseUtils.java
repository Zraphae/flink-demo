package cn.com.my.common.utils;

import cn.com.my.common.model.OGGMessage;
import com.google.common.base.Joiner;
import com.google.gson.JsonObject;

import java.util.Map;

public class HBaseUtils {

    public static final String DELETE_FLAG = "DELETE_FLAG";
    public static final String NULL_STRING = "\\N";

    public static String getHBaseRowKey(OGGMessage oggMessage, String primaryKeyName) {

        Map<String, String> keyValues = oggMessage.getKeyValues();
        String primaryValues = getPrimaryValues(primaryKeyName, keyValues);
        return primaryValues;
    }


    public static String getPrimaryValues(String primaryKeyName, Map<String, String> objKeyValue) {

        String[] primaryKeys = primaryKeyName.split(",");
        String[] keyValues = new String[primaryKeys.length];
        for (int index = 0; index < primaryKeys.length; index++) {
            String keyValue = objKeyValue.get(primaryKeys[index]);
            keyValues[index] = keyValue;
        }
        return Joiner.on("_").join(keyValues);
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
