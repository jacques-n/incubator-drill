package org.apache.drill.exec.rpc.data;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.RpcOutcomeListener;

public interface DataTunnel {

  public abstract void sendRecordBatch(RpcOutcomeListener<Ack> outcomeListener, FragmentWritableBatch batch);

  public abstract DrillRpcFuture<Ack> sendRecordBatch(FragmentContext context, FragmentWritableBatch batch);

}