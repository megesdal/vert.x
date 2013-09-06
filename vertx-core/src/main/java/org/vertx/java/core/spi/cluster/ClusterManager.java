/*
 * Copyright 2011-2012 the original author or authors.
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

package org.vertx.java.core.spi.cluster;


import java.util.List;
import java.util.Map;

/**
 *
 * A cluster provider for Vert.x must implement this interface.
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public interface ClusterManager {

  <K, V> AsyncMultiMap<K, V> getAsyncMultiMap(String name);

  <K, V> AsyncMap<K, V> getAsyncMap(String name);

  <K, V> Map<K, V> getSyncMap(String name);

  String getNodeID();

  List<String> getNodes();

  void setNodeListener(NodeListener listener);

  void join();

  void leave();
}
