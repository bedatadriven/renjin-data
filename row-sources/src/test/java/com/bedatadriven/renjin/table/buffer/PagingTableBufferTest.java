package com.bedatadriven.renjin.table.buffer;


import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class PagingTableBufferTest {

  @Test
  public void sequentialTest() {
    PagingTableBuffer buffer = new PagingTableBuffer(new SimpleRowSource());
    for(int i=0;i!=10000;++i) {
      assertThat(buffer.getDouble(i, 0), equalTo((double)i));
    }
  }

  @Test
  public void randomAccessTest() {
    PagingTableBuffer buffer = new PagingTableBuffer(new SimpleRowSource());
    assertThat(buffer.getDouble(0, 0), equalTo(0d));
    assertThat(buffer.getDouble(4500, 0), equalTo(4500d));
    assertThat(buffer.getDouble(200, 0), equalTo(200d));
    assertThat(buffer.getDouble(10000, 0), equalTo(10000d));
  }
}
