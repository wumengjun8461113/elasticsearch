package org.elasticsearch.http;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * A helper exception to mark an incoming connection as potentially being HTTP
 * so an appropriate error code can be returned
 */
public class HttpOnTransportException extends ElasticsearchException {

    public HttpOnTransportException(String msg) {
        super(msg);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

    public HttpOnTransportException(StreamInput in) throws IOException {
        super(in);
    }
}
