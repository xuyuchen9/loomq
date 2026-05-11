package com.loomq.spi;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * WAL 读取访问器。
 *
 * 内核通过此接口暴露 WAL 的读取能力，供服务层（如 ReplicationManager）
 * 读取 WAL 记录用于副本同步、追赶等场景。
 *
 * 默认由 SimpleWalWriter 实现。
 */
public interface WalAccessor {

    /** 当前写入位置（全局偏移） */
    long getWritePosition();

    /** 已刷盘位置（全局偏移） */
    long getFlushedPosition();

    /**
     * 按全局位置读取一条 WAL 记录，校验 CRC 后返回 payload。
     *
     * @param position     WAL 全局起始位置
     * @param recordLength 整条记录的长度
     * @return payload 字节数组
     * @throws IOException 读取失败或 CRC 校验不通过
     */
    byte[] readRecord(long position, int recordLength) throws IOException;

    /**
     * 获取所有 WAL 段文件列表。
     *
     * @return 按序号排序的段文件信息
     */
    List<WalSegment> listSegments();

    /**
     * 截断指定全局偏移之前的所有 WAL 段文件（释放磁盘空间）。
     * 仅在对应偏移已被快照持久化后调用。
     *
     * @param globalOffset 截断点（该位置之前的数据可安全删除）
     */
    void truncateBefore(long globalOffset);

    /**
     * WAL 段文件信息。
     */
    record WalSegment(int index, Path path, long startOffset, long endOffset, long size) {}
}
