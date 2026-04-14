package com.loomq.http.netty;

import io.netty.buffer.ByteBuf;

/**
 * 可直接序列化到 ByteBuf 的响应接口
 *
 * 实现此接口的响应对象可绕过 Jackson，直接写入 ByteBuf，
 * 实现零拷贝序列化优化。
 *
 * @author loomq
 * @since v0.6.0
 */
public interface DirectSerializedResponse {

    /**
     * 直接序列化到 ByteBuf
     *
     * @param buf 目标 ByteBuf，已预分配
     */
    void writeTo(ByteBuf buf);

    /**
     * 预估序列化后大小（用于 ByteBuf 预分配）
     *
     * @return 预估字节数
     */
    int estimateSize();
}
