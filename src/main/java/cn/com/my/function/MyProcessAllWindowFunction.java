package cn.com.my.function;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.util.Collector;


@Slf4j
public class MyProcessAllWindowFunction extends ProcessAllWindowFunction {

    @Override
    public void process(Context context, Iterable elements, Collector out) throws Exception {
        log.info("====>{}", elements.iterator().next());
        out.collect("123");
    }
}
