package com.loomq.http.netty;

import com.loomq.api.ErrorResponse;
import io.netty.buffer.ByteBuf;

/**
 * 带 HTTP 状态码的错误响应。
 *
 * 这是失败路径的轻量直出包装，避免先组 Map 再走 Jackson。
 */
public record HttpErrorResponse(int status, ErrorResponse error) implements DirectSerializedResponse {

    @Override
    public void writeTo(ByteBuf buf) {
        HttpErrorResponseSerializer.write(this, buf);
    }

    @Override
    public int estimateSize() {
        return HttpErrorResponseSerializer.estimateSize(this);
    }
}
