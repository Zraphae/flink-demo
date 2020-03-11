package cn.com.my.common.utils;

import com.google.gson.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Type;

@Slf4j
public class StringConverter implements JsonSerializer<String>, JsonDeserializer<String> {


    public JsonElement serialize(String src, Type typeOfSrc, JsonSerializationContext context) {
        if (src == null || StringUtils.isBlank(src)) {
            return new JsonPrimitive("\\N");
        } else {
            return new JsonPrimitive(src.toString());
        }
    }

    public String deserialize(JsonElement json, Type typeOfT,
                              JsonDeserializationContext context)
            throws JsonParseException {
        return json.getAsJsonPrimitive().getAsString();
    }
}