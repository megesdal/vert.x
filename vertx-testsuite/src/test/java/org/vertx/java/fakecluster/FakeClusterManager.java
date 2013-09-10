package org.vertx.java.fakecluster;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.spi.Action;
import org.vertx.java.core.spi.VertxSPI;
import org.vertx.java.core.spi.cluster.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
 * Copyright 2013 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */
public class FakeClusterManager implements ClusterManager {

  private static LinkedHashMap<String, FakeClusterManager> nodes = new LinkedHashMap<>();

  private static List<NodeListener> nodeListeners = new ArrayList<>();

  private String nodeID;

  private NodeListener nodeListener;

  private static ConcurrentMap<String, Map> syncMaps = new ConcurrentHashMap<>();
  private static ConcurrentMap<String, AsyncMap> asyncMaps = new ConcurrentHashMap<>();
  private static ConcurrentMap<String, AsyncMultiMap> asyncMultiMaps = new ConcurrentHashMap<>();

  private static ExecutorService executorService = Executors.newCachedThreadPool();

  private VertxSPI vertx;

  public FakeClusterManager(VertxSPI vertx) {
    this.vertx = vertx;
  }

  private static synchronized void doJoin(String nodeID, FakeClusterManager node) {
    if (nodes.containsKey(nodeID)) {
      throw new IllegalStateException("Node has already joined!");
    }
    nodes.put(nodeID, node);
    for (NodeListener listener: nodeListeners) {
      listener.nodeAdded(nodeID);
    }
  }

  private static synchronized void doLeave(String nodeID) {
    if (!nodes.containsKey(nodeID)) {
      throw new IllegalStateException("Node hasn't joined!");
    }
    nodes.remove(nodeID);
    for (NodeListener listener: nodeListeners) {
      listener.nodeLeft(nodeID);
    }

  }

  private static synchronized void doAddNodeListener(NodeListener listener) {
    if (nodeListeners.contains(listener)) {
      throw new IllegalStateException("Listener already registered!");
    }
    nodeListeners.add(listener);
  }

  private static synchronized void doRemoveNodeListener(NodeListener listener) {
    if (!nodeListeners.contains(listener)) {
      throw new IllegalStateException("Listener not registered!");
    }
    nodeListeners.remove(listener);
  }

  @Override
  public <K, V> AsyncMultiMap<K, V> getAsyncMultiMap(String name) {
    AsyncMultiMap<K, V> map = (AsyncMultiMap<K, V>)asyncMultiMaps.get(name);
    if (map == null) {
      map = new FakeAsyncMultiMap<>();
      AsyncMultiMap<K, V> prevMap = (AsyncMultiMap<K, V>)asyncMultiMaps.putIfAbsent(name, map);
      if (prevMap != null) {
        map = prevMap;
      }
    }
    return map;
  }

  @Override
  public <K, V> AsyncMap<K, V> getAsyncMap(String name) {
    AsyncMap<K, V> map = (AsyncMap<K, V>)asyncMaps.get(name);
    if (map == null) {
      map = new FakeAsyncMap<>();
      AsyncMap<K, V> prevMap = (AsyncMap<K, V>)asyncMaps.putIfAbsent(name, map);
      if (prevMap != null) {
        map = prevMap;
      }
    }
    return map;
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    Map<K, V> map = (Map<K, V>)syncMaps.get(name);
    if (map == null) {
      map = new ConcurrentHashMap<>();
      Map<K, V> prevMap = (Map<K, V>)syncMaps.putIfAbsent(name, map);
      if (prevMap != null) {
        map = prevMap;
      }
    }
    return map;
  }

  @Override
  public String getNodeID() {
    return nodeID;
  }

  @Override
  public List<String> getNodes() {
    synchronized (FakeClusterManager.class) {
      return new ArrayList<>(nodes.keySet());
    }
  }

  @Override
  public void nodeListener(NodeListener listener) {
    doAddNodeListener(listener);
    this.nodeListener = listener;
  }

  @Override
  public void join() {
    this.nodeID = UUID.randomUUID().toString();
    doJoin(nodeID, this);
  }

  @Override
  public void leave() {
    if (nodeID == null) {
      // Not joined
      return;
    }
    if (nodeListener != null) {
      doRemoveNodeListener(nodeListener);
      nodeListener = null;
    }
    doLeave(nodeID);
    this.nodeID = null;
  }

  private class FakeAsyncMap<K, V> implements AsyncMap<K, V> {

    private Map<K, V> map = new ConcurrentHashMap<>();

    @Override
    public void get(final K k, Handler<AsyncResult<V>> asyncResultHandler) {
      vertx.executeBlocking(new Action<V>() {
        public V perform() {
          return map.get(k);
        }
      }, asyncResultHandler);
    }

    @Override
    public void put(final K k, final V v, Handler<AsyncResult<Void>> completionHandler) {
      vertx.executeBlocking(new Action<Void>() {
        public Void perform() {
          map.put(k, v);
          return null;
        }
      }, completionHandler);
    }

    @Override
    public void remove(final K k, Handler<AsyncResult<Void>> completionHandler) {
      vertx.executeBlocking(new Action<Void>() {
        public Void perform() {
          map.remove(k);
          return null;
        }
      }, completionHandler);
    }
  }

  private class FakeAsyncMultiMap<K, V> implements AsyncMultiMap<K, V> {

    private ConcurrentMap<K, ChoosableSet<V>> map = new ConcurrentHashMap<>();

    @Override
    public void add(final K k, final V v, Handler<AsyncResult<Void>> completionHandler) {
      vertx.executeBlocking(new Action<Void>() {
        public Void perform() {
          ChoosableSet<V> vals = map.get(k);
          if (vals == null) {
            vals = new ChoosableSet<>(1);
            ChoosableSet<V> prevVals = map.putIfAbsent(k, vals);
            if (prevVals != null) {
              vals = prevVals;
            }
          }
          vals.add(v);
          return null;
        }
      }, completionHandler);
    }

    @Override
    public void get(final K k, Handler<AsyncResult<ChoosableSet<V>>> asyncResultHandler) {
      vertx.executeBlocking(new Action<ChoosableSet<V>>() {
        public ChoosableSet<V> perform() {
          return map.get(k);
        }
      }, asyncResultHandler);
    }

    @Override
    public void remove(final K k, final V v, Handler<AsyncResult<Void>> completionHandler) {
      vertx.executeBlocking(new Action<Void>() {
        public Void perform() {
          ChoosableSet<V> vals = map.get(k);
          if (vals != null) {
            vals.remove(v);
            if (vals.isEmpty()) {
              map.remove(k);
            }
          }
          return null;
        }
      }, completionHandler);
    }

    @Override
    public void removeAllForValue(final V v, Handler<AsyncResult<Void>> completionHandler) {
      vertx.executeBlocking(new Action<Void>() {
        public Void perform() {
          Iterator<Map.Entry<K, ChoosableSet<V>>> mapIter = map.entrySet().iterator();
          while (mapIter.hasNext()) {
            Map.Entry<K, ChoosableSet<V>> entry = mapIter.next();
            ChoosableSet<V> vals = entry.getValue();
            Iterator<V> iter = vals.iterator();
            while (iter.hasNext()) {
              V val = iter.next();
              if (val.equals(v)) {
                iter.remove();
              }
            }
            if (vals.isEmpty()) {
              mapIter.remove();
            }
          }
          return null;
        }
      }, completionHandler);
    }
  }
}
