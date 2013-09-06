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

package org.vertx.java.core.impl.ha;

import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;
import org.vertx.java.core.spi.cluster.ClusterManager;
import org.vertx.java.core.spi.cluster.NodeListener;

import java.util.*;

/**
 *
 * Handles HA
 *
 * This class should always be used from a worker thread - not an event loop, as it it does blocking stuff
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class HAManager {

  private static final Logger log = LoggerFactory.getLogger(HAManager.class);
  private static final String MAP_NAME = "__vertx.clusterMap";

  private final ClusterManager clusterManager;
  private final int quorumSize;
  private final String group;
  private final JsonObject haInfo;
  private final JsonArray haMods;
  private final Map<String, String> clusterMap;
  private final String nodeID;
  private boolean attainedQuorum;
  private Handler<Boolean> quorumHandler;
  private Handler<JsonObject> failoverHandler;
  private Handler<Boolean> failoverCompleteHandler;

  public HAManager(ClusterManager clusterManager, int quorumSize, String group) {
    this.clusterManager = clusterManager;
    this.quorumSize = quorumSize;
    this.group = group;
    this.haInfo = new JsonObject();
    this.haMods = new JsonArray();
    haInfo.putArray("mods", haMods);
    haInfo.putString("group", group == null ? "__DEFAULT__" : group);
    this.clusterMap = clusterManager.getSyncMap(MAP_NAME);
    this.nodeID = clusterManager.getNodeID();
    clusterManager.setNodeListener(listener);
    clusterMap.put(nodeID, haInfo.encode());
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
        String sclusterInfo = clusterMap.get(leftNodeID);
        if (sclusterInfo == null) {
          // Clean close - do nothing
        } else {
          checkFailover(leftNodeID, new JsonObject(sclusterInfo));
        }

        // We also check for and potentially resume any previous failovers that might have failed
        // We can determine this if there any ids in the clustermap which aren't in the node list
        Collection<String> clusterMapNodes = clusterMap.keySet();
        List<String> nodes = clusterManager.getNodes();

        for (Map.Entry<String, String> entry: clusterMap.entrySet()) {
          if (!nodes.contains(entry.getKey())) {
            checkFailover(entry.getKey(), new JsonObject(entry.getValue()));
          }
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

  public void failoverCompleteHandler(Handler<Boolean> failoverCompleteHandler) {
    this.failoverCompleteHandler = failoverCompleteHandler;
  }

  public void addToHA(String deploymentID, String moduleName, JsonObject conf, int instances) {
    JsonObject moduleConf = new JsonObject().putString("dep_id", deploymentID);
    moduleConf.putString("module_name", moduleName);
    if (conf != null) {
      moduleConf.putObject("conf", conf);
    }
    moduleConf.putNumber("instances", instances);
    haMods.addObject(moduleConf);
    clusterMap.put(nodeID, haInfo.encode());
  }

  public void removeFromHA(String depID) {
    Iterator<Object> iter = haMods.iterator();
    while (iter.hasNext()) {
      Object obj = iter.next();
      JsonObject mod = (JsonObject)obj;
      if (mod.getString("dep_id").equals(depID)) {
        iter.remove();
      }
    }
    clusterMap.put(nodeID, haInfo.encode());
  }

  public void stop() {
    clusterMap.remove(nodeID);
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
    try {
      JsonArray apps = theHAInfo.getArray("mods");
      String group = theHAInfo.getString("group");
      String chosen = chooseHashedNode(group, failedNodeID.hashCode());
      if (chosen != null && chosen.equals(this.nodeID)) {
        log.info("Node " + failedNodeID + " has failed. This node will deploy " + apps.size() + " deployments from that node.");
        if (apps != null) {
          for (Object obj: apps) {
            JsonObject app = (JsonObject)obj;
            failoverHandler.handle(app);
          }
        }
        // Failover is complete! We can now remove the failed node from the cluster map
        clusterMap.remove(failedNodeID);
        if (failoverCompleteHandler != null) {
          failoverCompleteHandler.handle(true);
        }
      }
    } catch (Throwable t) {
      log.error("Failed to handle failover", t);
      if (failoverCompleteHandler != null) {
        failoverCompleteHandler.handle(false);
      }
    }
  }

  /*
  We use the hashcode of the failed nodeid to choose a node to failover onto.
  Each node is guaranteed to see the exact list of nodes in the cluster for the exact same membership event
  Therefore each node will compute the same value.
  If the computed node equals the current node, the node knows that it is the one to handle failover for the
  deployment
   */
  private String chooseHashedNode(String group, int hashCode) {
    List<String> nodes = clusterManager.getNodes();
    ArrayList<String> matchingMembers = new ArrayList<>();
    for (String node: nodes) {
      String sclusterInfo = clusterMap.get(node);
      if (sclusterInfo == null) {
        throw new IllegalStateException("Can't find node in map");
      }
      JsonObject clusterInfo = new JsonObject(sclusterInfo);
      String memberGroup = clusterInfo.getString("group");
      if (group.equals(memberGroup)) {
        matchingMembers.add(node);
      }
    }
    if (!matchingMembers.isEmpty()) {
      // Hashcodes can be -ve so make it positive
      long absHash = (long)hashCode + Integer.MAX_VALUE;
      long lpos = absHash % matchingMembers.size();
      return matchingMembers.get((int)lpos);
    } else {
      return null;
    }
  }


}
