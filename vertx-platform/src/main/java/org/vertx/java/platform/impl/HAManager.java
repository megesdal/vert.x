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

package org.vertx.java.platform.impl;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.VertxException;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;
import org.vertx.java.core.spi.cluster.ClusterManager;
import org.vertx.java.core.spi.cluster.NodeListener;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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

  private final PlatformManagerInternal platformManager;
  private final ClusterManager clusterManager;
  private final int quorumSize;
  private final String group;
  private final JsonObject haInfo;
  private final JsonArray haMods;
  private final Map<String, String> clusterMap;
  private final String nodeID;
  private final List<Runnable> toDeployOnQuorum = new ArrayList<>();
  private boolean attainedQuorum;
  private Handler<Boolean> failoverCompleteHandler;
  private boolean failDuringFailover;

  // We use a semaphore to ensure that HA modules aren't deployed at the same time as modules are undeployed
  // due to quorum being los, which might result in modules being active when no quorum
  private final Semaphore sem = new Semaphore(1);

  public HAManager(PlatformManagerInternal platformManager, ClusterManager clusterManager, int quorumSize, String group) {
    this.platformManager = platformManager;
    this.clusterManager = clusterManager;
    this.quorumSize = quorumSize;
    this.group = group;
    this.haInfo = new JsonObject();
    this.haMods = new JsonArray();
    haInfo.putArray("mods", haMods);
    haInfo.putString("group", group == null ? "__DEFAULT__" : group);
    this.clusterMap = clusterManager.getSyncMap(MAP_NAME);
    this.nodeID = clusterManager.getNodeID();
    clusterManager.setNodeListener(new NodeListener() {
      @Override
      public void nodeAdded(String nodeID) {
        HAManager.this.nodeAdded(nodeID);
      }

      @Override
      public void nodeLeft(String leftNodeID) {
        HAManager.this.nodeLeft(leftNodeID);
      }
    });
    clusterMap.put(nodeID, haInfo.encode());
    attainedQuorum = quorumSize == 0;
  }

  // Set a handler that will be called when failover is complete - used in testing
  public void failoverCompleteHandler(Handler<Boolean> failoverCompleteHandler) {
    this.failoverCompleteHandler = failoverCompleteHandler;
  }

  // Remove the information on the deployment from the cluster - this is called when an HA module is undeployed
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

  // Deploy an HA module
  public synchronized void deployModule(final String moduleName, final JsonObject config, final int instances,
                                        final Handler<AsyncResult<String>> doneHandler) {

    if (attainedQuorum) {
      try {
        sem.acquire();
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
      final Handler<AsyncResult<String>> wrappedHandler = new Handler<AsyncResult<String>>() {
        @Override
        public void handle(AsyncResult<String> asyncResult) {
          if (asyncResult.succeeded()) {
            // Tell the other nodes of the cluster about the module for HA purposes
            addToHA(asyncResult.result(), moduleName, config, instances);
          }
          if (doneHandler != null) {
            doneHandler.handle(asyncResult);
          } else if (asyncResult.failed()) {
            log.error("Failed to deploy module", asyncResult.cause());
          }
          sem.release();
        }
      };
      platformManager.deployModuleInternal(moduleName, config, instances, true, wrappedHandler);
    } else {
      log.info("Quorum not attained. Deployment of module will be delayed until there's a quorum.");
      addToHADeployList(moduleName, config, instances, doneHandler);
    }
  }

  public void stop() {
    clusterMap.remove(nodeID);
  }

  public void failDuringFailover(boolean fail) {
    failDuringFailover = fail;
  }

  // A node has joined the cluster
  private void nodeAdded(String nodeID) {
    System.out.println("Node has been added");
    checkQuorum();
  }

  // A node has left the cluster
  private void nodeLeft(String leftNodeID) {
    checkQuorum();
    if (attainedQuorum) {

      // Check for failover

      String sclusterInfo = clusterMap.get(leftNodeID);
      if (sclusterInfo == null) {
        // Clean close - do nothing
      } else {
        checkFailover(leftNodeID, new JsonObject(sclusterInfo));
      }

      // We also check for and potentially resume any previous failovers that might have failed
      // We can determine this if there any ids in the cluster map which aren't in the node list
      List<String> nodes = clusterManager.getNodes();

      for (Map.Entry<String, String> entry: clusterMap.entrySet()) {
        if (!nodes.contains(entry.getKey())) {
          checkFailover(entry.getKey(), new JsonObject(entry.getValue()));
        }
      }
    }
  }

  // Check if there is a quorum for our group
  private synchronized void checkQuorum() {

    List<String> nodes = clusterManager.getNodes();
    System.out.println("There are " + nodes.size() +" nodes");
    int count = 0;
    for (String node: nodes) {
      String json = clusterMap.get(node);
      System.out.println("json is " + json);
      if (json != null) {
        JsonObject clusterInfo = new JsonObject(json);
        String group = clusterInfo.getString("group");
        if (group.equals(this.group)) {
          count++;
        }
      }
    }
    System.out.println("count is " + count);
    boolean attained = count >= quorumSize;
    if (!attainedQuorum && attained) {
      // A quorum has been attained so we can deploy any currently undeployed HA deployments
      deployHADeployments();
    } else if (attainedQuorum && !attained) {
      // We had a quorum but we lost it - we must undeploy any HA deployments
      undeployHADeployments();
    }
    this.attainedQuorum = attained;
  }

  // Add some information on a deployment in the cluster so other nodes know about it
  private void addToHA(String deploymentID, String moduleName, JsonObject conf, int instances) {
    JsonObject moduleConf = new JsonObject().putString("dep_id", deploymentID);
    moduleConf.putString("module_name", moduleName);
    if (conf != null) {
      moduleConf.putObject("conf", conf);
    }
    moduleConf.putNumber("instances", instances);
    haMods.addObject(moduleConf);
    clusterMap.put(nodeID, haInfo.encode());
  }

  // Add the deployment to an internal list of deployments - these will be executed when a quorum is attained
  private void addToHADeployList(final String moduleName, final JsonObject config, final int instances,
                                 final Handler<AsyncResult<String>> doneHandler) {
    addToHADeployList(new Runnable() {
      public void run() {
        platformManager.deployModule(moduleName, config, instances, true, doneHandler);
      }
    });
  }

  // Undeploy all HA deployments
  // Note - this methods blocks until undeployment is complete and we use a semaphore to ensure no deployment occurs
  // during this period
  private void undeployHADeployments() {
    try {
      sem.acquire();
      for (final Map.Entry<String, Deployment> entry: platformManager.deployments().entrySet()) {
        if (entry.getValue().ha) {
          final CountDownLatch latch = new CountDownLatch(1);
          platformManager.undeploy(entry.getKey(), new AsyncResultHandler<Void>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.succeeded()) {
                final Deployment dep = entry.getValue();
                addToHADeployList(dep.modID.toString(), dep.config, dep.instances, new AsyncResultHandler<String>() {
                  @Override
                  public void handle(AsyncResult<String> result) {
                    if (result.succeeded()) {
                      log.info("Successfully redeployed module " + entry.getValue().modID + " after quorum was re-attained");
                    } else {
                      log.error("Failed to redeploy module " + entry.getValue().modID + " after quorum was re-attained", result.cause());
                    }
                  }
                });
              } else {
                log.error("Failed to undeploy deployment on lost quorum", result.cause());
              }
              latch.countDown();
            }
          });
          try {
            if (!latch.await(300, TimeUnit.SECONDS)) {
              throw new IllegalStateException("Timed out waiting to undeploy");
            }
          } catch (InterruptedException e) {
            throw new IllegalStateException(e);
          }
        }
      }
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    } finally {
      sem.release();
    }
  }

  private void deployHADeployments() {
    for (final Runnable task: toDeployOnQuorum) {
      try {
        task.run();
      } catch (Throwable t) {
        log.error("Failed to run redeployment task", t);
      }
    }
    toDeployOnQuorum.clear();
  }

  private synchronized void addToHADeployList(Runnable task) {
    toDeployOnQuorum.add(task);
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
            handleFailover(app);
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

  // Handle the failover of a module
  private void handleFailover(JsonObject failedModule) {
    System.out.println("Handling failover of " + failedModule);
    if (failDuringFailover) {
      throw new VertxException("Oops!");
    }
    // This method must block until the failover is complete - i.e. the module is successfully redeployed
    final String moduleName = failedModule.getString("module_name");
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Throwable> err = new AtomicReference<>();
    // Now deploy this module on this node
    platformManager.deployModule(moduleName, failedModule.getObject("conf"), failedModule.getInteger("instances"), true, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        if (result.succeeded()) {
          log.info("Successfully redeployed module " + moduleName + " after failover");
        } else {
          log.error("Failed to redeploy module after failover", result.cause());
          err.set(result.cause());
        }
        latch.countDown();
        Throwable t = err.get();
        if (t != null) {
          throw new VertxException(t);
        }
      }
    });
    try {
      if (!latch.await(10, TimeUnit.SECONDS)) {
        throw new VertxException("Timed out waiting for redeploy on failover");
      }
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
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
