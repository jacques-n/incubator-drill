/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.rpc.data;

import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.DrillBuf;
import io.netty.channel.EventLoopGroup;

import java.util.concurrent.Semaphore;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitData.FragmentRecordBatch;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.DrillRpcFutureImpl;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.ResponseSender;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;

public class LocalDataTunnel implements DataTunnel {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LocalDataTunnel.class);

  private final DataServer dataServer;
  private final EventLoopGroup eventGroup;
  private final Semaphore sendingSemaphore = new Semaphore(3);

  public LocalDataTunnel(DataServer dataServer, EventLoopGroup eventGroup) {
    super();
    this.dataServer = dataServer;
    this.eventGroup = eventGroup;
  }

  @Override
  public void sendRecordBatch(RpcOutcomeListener<Ack> outcomeListener, FragmentWritableBatch batch) {
    final DrillBuf buf;
    if(batch.getBuffers().length == 0){
      buf = null;
    }else{
      final CompositeByteBuf comp = new CompositeByteBuf(batch.getBuffers()[0].alloc(), true, batch.getBuffers().length);
      for(DrillBuf b : batch.getBuffers()){
        comp.addComponent(b);
      }
      buf = new DrillBuf(batch.getBuffers()[0].getAllocator(), comp);
    }
    try {
      sendingSemaphore.acquire();
      eventGroup.submit(new Sending(batch.getHeader(), buf, outcomeListener));
    } catch (InterruptedException e) {
      outcomeListener.failed(new RpcException("Interrupted while trying to get sending semaphore.", e));
    }
  }

  @Override
  public DrillRpcFuture<Ack> sendRecordBatch(FragmentContext context, FragmentWritableBatch batch) {
    DrillRpcFutureImpl<Ack> future = new DrillRpcFutureImpl<>();
    sendRecordBatch(future, batch);
    return future;
  }

  private class Sending implements Runnable, ResponseSender {
    final FragmentRecordBatch fragRecordBatch;
    final DrillBuf body;
    final RpcOutcomeListener<Ack> outcomeListener;

    public Sending(FragmentRecordBatch fragRecordBatch, DrillBuf body, RpcOutcomeListener<Ack> outcomeListener) {
      super();
      this.fragRecordBatch = fragRecordBatch;
      this.body = body;
      this.outcomeListener = outcomeListener;
    }

    @Override
    public void run() {
      try {
        dataServer.receiveBatch(fragRecordBatch, body, this);
      } catch (Exception e) {
        logger.error("Failure while sending local batch.", e);
      } finally {
        sendingSemaphore.release();
      }
    }

    @Override
    public void send(Response r) {
      outcomeListener.success((Ack) r.pBody, null);
    }


  }


}
