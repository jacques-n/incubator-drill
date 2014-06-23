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
package org.apache.drill.exec.rpc;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.internal.SystemPropertyUtil;

import java.util.Locale;

public class PlatformChecker {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlatformChecker.class);

  public static final boolean SUPPORTS_EPOLL = false;

  static{
//    String name = SystemPropertyUtil.get("os.name").toLowerCase(Locale.UK).trim();
//    if (!name.startsWith("linux")) {
//      SUPPORTS_EPOLL = false;
//    }else{
//      SUPPORTS_EPOLL = true;
//    }
  }

  public static Class<? extends ServerSocketChannel> getServerSocketChannel(){
    if(SUPPORTS_EPOLL){
      return EpollServerSocketChannel.class;
    }else{
      return NioServerSocketChannel.class;
    }
  }

  public static Class<? extends SocketChannel> getClientSocketChannel(){
    if(SUPPORTS_EPOLL){
      return EpollSocketChannel.class;
    }else{
      return NioSocketChannel.class;
    }
  }

  public static EventLoopGroup createEventLoopGroup(int nThreads, String prefix) {
     if(SUPPORTS_EPOLL){
       return new EpollEventLoopGroup(nThreads, new NamedThreadFactory(prefix));
     }else{
       return new NioEventLoopGroup(nThreads, new NamedThreadFactory(prefix));
     }
  }
}
