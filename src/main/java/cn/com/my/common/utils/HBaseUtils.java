package cn.com.my.common.utils;

import cn.com.my.common.model.OGGMessage;
import com.google.common.base.Joiner;
import com.google.gson.JsonObject;

public class HBaseUtils {

    public static String getHBaseRowKey(OGGMessage oggMessage, String primaryKeyName) {

        JsonObject jsonObject = GsonUtil.parse2JsonObj(oggMessage.getData().toString());

        String[] primaryKeys = primaryKeyName.split(",");
        String[] keyValues = new String[primaryKeys.length];
        for(int index=0; index<primaryKeys.length; index++){
            String keyValue = jsonObject.get(primaryKeys[index]).getAsString();
            keyValues[index] = keyValue;
        }
        String hbaseRowKey = Joiner.on("_").join(keyValues);

        return hbaseRowKey;
    }
}
