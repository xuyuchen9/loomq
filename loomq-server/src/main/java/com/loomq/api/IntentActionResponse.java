package com.loomq.api;

import com.loomq.http.netty.DirectSerializedResponse;
import com.loomq.http.netty.IntentActionResponseSerializer;
import io.netty.buffer.ByteBuf;

/**
 * 轻量 intent 动作响应。
 *
 * 用于 fire-now 这类只返回 intentId + status 的高频简单响应，避免 Map + Jackson。
 */
public record IntentActionResponse(String intentId, String status) implements DirectSerializedResponse {

    @Override
    public void writeTo(ByteBuf buf) {
        IntentActionResponseSerializer.write(this, buf);
    }

    @Override
    public int estimateSize() {
        return IntentActionResponseSerializer.estimateSize(this);
    }
}
