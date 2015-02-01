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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;
import io.netty.buffer.UnsafeDirectLittleEndian;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GenericFutureListener;

import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.BitData.BitClientHandshake;
import org.apache.drill.exec.proto.BitData.BitServerHandshake;
import org.apache.drill.exec.proto.BitData.FragmentRecordBatch;
import org.apache.drill.exec.proto.BitData.RpcType;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.RpcChannel;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.Acks;
import org.apache.drill.exec.rpc.BasicServer;
import org.apache.drill.exec.rpc.OutOfMemoryHandler;
import org.apache.drill.exec.rpc.ProtobufLengthDecoder;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.ResponseSender;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.control.WorkEventBus;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.fragment.FragmentManager;

import com.google.protobuf.MessageLite;

public class DataServer extends BasicServer<RpcType, BitServerConnection> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DataServer.class);

  private volatile ProxyCloseHandler proxyCloseHandler;
  private final BootStrapContext context;
  private final WorkEventBus workBus;
  private final DataResponseHandler dataHandler;

  public DataServer(BootStrapContext context, WorkEventBus workBus, DataResponseHandler dataHandler) {
    super(DataRpcConfig.MAPPING, context.getAllocator().getUnderlyingAllocator(), context.getBitLoopGroup());
    this.context = context;
    this.workBus = workBus;
    this.dataHandler = dataHandler;
  }

  @Override
  public MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    return DataDefaultInstanceHandler.getResponseDefaultInstanceServer(rpcType);
  }

  @Override
  protected GenericFutureListener<ChannelFuture> getCloseHandler(BitServerConnection connection) {
    this.proxyCloseHandler = new ProxyCloseHandler(super.getCloseHandler(connection));
    return proxyCloseHandler;
  }

  @Override
  public BitServerConnection initRemoteConnection(Channel channel) {
    return new BitServerConnection(channel, context.getAllocator());
  }

  @Override
  protected ServerHandshakeHandler<BitClientHandshake> getHandshakeHandler(final BitServerConnection connection) {
    return new ServerHandshakeHandler<BitClientHandshake>(RpcType.HANDSHAKE, BitClientHandshake.PARSER) {

      @Override
      public MessageLite getHandshakeResponse(BitClientHandshake inbound) throws Exception {
        // logger.debug("Handling handshake from other bit. {}", inbound);
        if (inbound.getRpcVersion() != DataRpcConfig.RPC_VERSION) {
          throw new RpcException(String.format("Invalid rpc version.  Expected %d, actual %d.",
              inbound.getRpcVersion(), DataRpcConfig.RPC_VERSION));
        }
        if (inbound.getChannel() != RpcChannel.BIT_DATA) {
          throw new RpcException(String.format("Invalid NodeMode.  Expected BIT_DATA but received %s.",
              inbound.getChannel()));
        }

        return BitServerHandshake.newBuilder().setRpcVersion(DataRpcConfig.RPC_VERSION).build();
      }

    };
  }

  private final static FragmentRecordBatch OOM_FRAGMENT = FragmentRecordBatch.newBuilder().setIsOutOfMemory(true).build();


  private FragmentHandle getHandle(FragmentRecordBatch batch, int index){
    return FragmentHandle.newBuilder()
        .setQueryId(batch.getQueryId())
        .setMajorFragmentId(batch.getReceivingMajorFragmentId())
        .setMinorFragmentId(batch.getReceivingMinorFragmentId(index))
        .build();
  }


  @Override
  protected void handle(BitServerConnection connection, int rpcType, ByteBuf pBody, ByteBuf body, ResponseSender sender) throws RpcException {
    assert rpcType == RpcType.REQ_RECORD_BATCH_VALUE;

    final FragmentRecordBatch fragmentBatch = get(pBody, FragmentRecordBatch.PARSER);
    final int targetCount = fragmentBatch.getReceivingMinorFragmentIdCount();

    Pointer<DrillBuf> out = new Pointer<DrillBuf>();

    try {

      if(body == null){

        for(int minor = 0; minor < targetCount; minor++){
          FragmentManager manager = workBus.getFragmentManager(getHandle(fragmentBatch, minor));
          if(manager != null){
            dataHandler.handle(connection, manager, fragmentBatch, null, sender);
          }
        }

      }else{

        for(int minor = 0; minor < targetCount; minor++){
          FragmentManager manager = workBus.getFragmentManager(getHandle(fragmentBatch, minor));
          if(manager == null){
            continue;
          }

          BufferAllocator allocator = manager.getFragmentContext().getAllocator();

          boolean withinMemoryEnvelope = allocator.takeOwnership((DrillBuf) body, out);

          if(!withinMemoryEnvelope){
            // if we over reserved, we need to add poison pill before batch.
            dataHandler.handle(connection, manager, OOM_FRAGMENT, null, null);
          }

          dataHandler.handle(connection, manager, fragmentBatch, out.value, sender);

          // make sure to release the reference count we have to the new buffer.
          // dataHandler.handle should have taken any ownership it needed.
          out.value.release();
        }
        out = null;
      }

    } catch (FragmentSetupException e) {
      logger.error("Failure while getting fragment manager. {}",
          QueryIdHelper.getQueryIdentifiers(fragmentBatch.getQueryId(),
              fragmentBatch.getReceivingMajorFragmentId(),
              fragmentBatch.getReceivingMinorFragmentIdList()));
      sender.send(new Response(RpcType.ACK, Acks.FAIL));
    } finally {
      if(out != null && out.value != null){
        out.value.release();
      }
    }
  }

  private class ProxyCloseHandler implements GenericFutureListener<ChannelFuture> {

    private volatile GenericFutureListener<ChannelFuture> handler;

    public ProxyCloseHandler(GenericFutureListener<ChannelFuture> handler) {
      super();
      this.handler = handler;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      handler.operationComplete(future);
    }

  }

  @Override
  public OutOfMemoryHandler getOutOfMemoryHandler() {
    return new OutOfMemoryHandler() {
      @Override
      public void handle() {
        dataHandler.informOutOfMemory();
      }
    };
  }

  @Override
  public ProtobufLengthDecoder getDecoder(BufferAllocator allocator, OutOfMemoryHandler outOfMemoryHandler) {
    return new DataProtobufLengthDecoder(allocator, outOfMemoryHandler);
  }

}
