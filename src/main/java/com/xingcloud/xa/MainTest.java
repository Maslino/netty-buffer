package com.xingcloud.xa;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * User: liuxiong
 * Date: 13-9-16
 * Time: 下午5:10
 */
public class MainTest {

  public static void main(String[] args){
    PooledByteBufAllocator allocator = new PooledByteBufAllocator(true);
    ByteBuf bb = allocator.directBuffer(10);
    bb.setZero(0, bb.capacity());
  }

}
