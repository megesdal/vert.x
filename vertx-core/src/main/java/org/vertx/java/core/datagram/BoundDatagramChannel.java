/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.vertx.java.core.datagram;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.streams.ExceptionSupport;

import java.net.InetSocketAddress;


/**
 * @author <a href="mailto:nmaurer@redhat.com">Norman Maurer</a>
 */
public interface BoundDatagramChannel extends DatagramChannel<BoundDatagramChannel>, ExceptionSupport<BoundDatagramChannel> {

  BoundDatagramChannel write(Buffer packet, InetSocketAddress remote,  Handler<AsyncResult<BoundDatagramChannel>> handler);
  BoundDatagramChannel write(String str, InetSocketAddress remote, Handler<AsyncResult<BoundDatagramChannel>> handler);
  BoundDatagramChannel write(String str, String enc, InetSocketAddress remote, Handler<AsyncResult<BoundDatagramChannel>> handler);
  /**
   * Set a data handler. As data is read, the handler will be called with the data.
   */
  BoundDatagramChannel dataHandler(Handler<DatagramPacket> packetHandler);
}
