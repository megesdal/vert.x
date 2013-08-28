package org.vertx.java.core.impl.ha;

import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.spi.cluster.ClusterManager;
import org.vertx.java.core.spi.cluster.NodeListener;

import java.util.*;

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
public class HAManager {

  private static final String MAP_NAME = "__vertx.clusterMap";

  private final ClusterManager clusterManager;
  private final int quorumSize;
  private final JsonObject haInfo;
  private final JsonArray haMods;
  private final Map<String, JsonObject> clusterMap;
  private final String nodeID;
  private boolean attainedQuorum;
  private Handler<Boolean> quorumHandler;
  private Handler<JsonObject> failoverHandler;

  public HAManager(ClusterManager clusterManager, int quorumSize, String group) {
    this.clusterManager = clusterManager;
    this.quorumSize = quorumSize;
    this.haInfo = new JsonObject();
    haInfo.putString("group", group != null ? group : "_DEFAULT_");
    this.haMods = new JsonArray();
    haInfo.putArray("mods", haMods);
    this.clusterMap = clusterManager.getMap(MAP_NAME);
    this.nodeID = clusterManager.getNodeID();
  }

  private final NodeListener listener = new NodeListener() {
    @Override
    public void nodeAdded(String nodeID) {
      checkQuorum();
    }

    @Override
    public void nodeLeft(String leftNodeID) {
      checkQuorum();
      if (attainedQuorum) {
        JsonObject clusterInfo = clusterMap.get(leftNodeID);
        if (clusterInfo == null) {
          // Clean close - do nothing
        } else {
          checkFailover(leftNodeID, clusterInfo);
        }
      }
    }
  };

  public void quorumHandler(Handler<Boolean> quorumHandler) {
    this.quorumHandler = quorumHandler;
  }

  public void failoverHandler(Handler<JsonObject> failoverHandler) {
    this.failoverHandler = failoverHandler;
  }

  public void addToHA(String moduleName, JsonObject conf, int instances, String group) {
    JsonObject moduleConf = new JsonObject().putString("module_name", moduleName);
    if (conf == null) {
      conf = new JsonObject();
    }
    moduleConf.putObject("conf", conf);
    moduleConf.putNumber("instances", instances);
    haMods.addObject(moduleConf);
    clusterMap.put(nodeID, haInfo);
  }

  public void removeFromHA(String moduleName, JsonObject conf, int instances) {
    Iterator<Object> iter = haMods.iterator();
    while (iter.hasNext()) {
      Object obj = iter.next();
      JsonObject mod = (JsonObject)obj;
      if (mod.getString("module_name").equals(moduleName) &&
          mod.getObject("conf").equals(conf) &&
          mod.getNumber("instances") == instances) {
        iter.remove();
      }
    }
    clusterMap.put(nodeID, haInfo);
  }

  private void checkQuorum() {
    boolean attained = clusterManager.getNodes().size() >= quorumSize;
    if (quorumHandler != null) {
      if (!attainedQuorum && attained) {
        quorumHandler.handle(true);
      } else if (attainedQuorum && !attained) {
        quorumHandler.handle(false);
      }
    }
    this.attainedQuorum = attained;
  }

  /*
  We work out which node will take over the failed node - the results of this calculation
  should be exactly the same on every node, so only one node will actually take it on
  Consequently it's crucial that the calculation done in chooseHashedNode takes place only locally
   */
  private void checkFailover(String failedNodeID, JsonObject theHAInfo) {
    JsonArray apps = theHAInfo.getArray("mods");
    String group = theHAInfo.getString("group");
    if (apps != null) {
      for (Object obj: apps) {
        JsonObject app = (JsonObject)obj;
        String moduleName = app.getString("module_name");
        String chosen = chooseHashedNode(group, moduleName.hashCode());
        if (chosen != null && chosen.equals(this.nodeID)) {
          System.out.println("node " + nodeID + " is handling failure of app " + moduleName + " from node " + failedNodeID);
          failoverHandler.handle(app);
          break;
        }
      }
    }
  }

  private String chooseHashedNode(String group, int hashCode) {
    List<String> nodes = clusterManager.getNodes();
    ArrayList<String> matchingMembers = new ArrayList<>();
    for (String node: nodes) {
      JsonObject clusterInfo = clusterMap.get(node);
      if (clusterInfo == null) {
        throw new IllegalStateException("Can't find node in map");
      }
      String memberGroup = clusterInfo.getString("group");
      if (group.equals(memberGroup)) {
        matchingMembers.add(node);
      }
    }
    if (!matchingMembers.isEmpty()) {
      int pos = hashCode % matchingMembers.size();
      return matchingMembers.get(pos);
    } else {
      return null;
    }
  }


}
