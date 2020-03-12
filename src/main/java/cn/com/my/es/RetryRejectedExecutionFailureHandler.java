package cn.com.my.es;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.util.ExceptionUtils;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Optional;

@Slf4j
public class RetryRejectedExecutionFailureHandler implements ActionRequestFailureHandler {

    @Override
    public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
        if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
            indexer.add(action);
        } else {
            if (ExceptionUtils.findThrowable(failure, SocketTimeoutException.class).isPresent()) {
                // 忽略写入超时，因为ElasticSearchSink 内部会重试请求，不需要抛出来去重启 flink job
                return;
            } else {
                Optional<IOException> exp = ExceptionUtils.findThrowable(failure, IOException.class);
                if (exp.isPresent()) {
                    IOException ioExp = exp.get();
                    if (ioExp != null && ioExp.getMessage() != null && ioExp.getMessage().contains("max retry timeout")) {
                        // request retries exceeded max retry timeout, throw failure, restart flink job
                        log.error(ioExp.getMessage());
                        throw failure;
                    }
                }
            }
            throw failure;
        }
    }
}