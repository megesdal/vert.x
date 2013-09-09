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

package org.vertx.java.tests.platform.ha;

import junit.framework.TestCase;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.impl.Deployment;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HATest extends TestCase {

  protected void setUp() throws Exception {
    super.setUp();
    System.setProperty("vertx.mods", "vertx-testsuite/src/test/mod-test");
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  private Random random = new Random();

  public void testSimpleNode0() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.killNode(0);
    cluster.closeCluster();
  }

  public void testSimpleNode1() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testSimpleNode2() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.killNode(2);
    cluster.closeCluster();
  }

  public void testMultiple() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")).addDeployment(new DepInfo("io.vertx~ha-test2~1.0")));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testMultipleSameModule() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")).addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testLots() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    NodeMods mods = new NodeMods();
    for (int i = 0; i < 100; i++) {
      mods.addDeployment(new DepInfo("io.vertx~ha-test1~1.0")).addDeployment(new DepInfo("io.vertx~ha-test2~1.0"));
    }
    cluster.deployMods(1, mods);
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testSimpleOtherNodesSameMod() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testSimpleOtherNodesDifferentMod() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test2~1.0")));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test2~1.0")));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testMultipleOtherNodesDifferentMod() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")).addDeployment(new DepInfo("io.vertx~ha-test2~1.0")));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testMultipleOtherNodesSameMod() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")).addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")).addDeployment(new DepInfo("io.vertx~ha-test2~1.0")));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")).addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testKillAllNoDeployments() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.killNode(1);
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testSimpleMultipleKill() throws Exception {
    int clusterSize = 10;
    Cluster cluster = new Cluster(clusterSize);
    cluster.createCluster();
    cluster.deployMods(clusterSize / 2, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    killAllClusterSequentially(cluster, clusterSize);
    cluster.closeCluster();
  }

  private void killAllClusterSequentially(Cluster cluster, int clusterSize) throws Exception {
    int pos = 0;
    for (int size = clusterSize; size > 1; size--) {
      cluster.killNode(pos);
      pos++;
      if (pos >= size - 1) {
        pos = 0;
      }
    }
  }

  public void testMultipleMultipleKill() throws Exception {
    int clusterSize = 10;
    Cluster cluster = new Cluster(clusterSize);
    cluster.createCluster();
    cluster.deployMods(clusterSize / 2, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")).addDeployment(new DepInfo("io.vertx~ha-test2~1.0")));
    killAllClusterSequentially(cluster, clusterSize);
    cluster.closeCluster();
  }

  private void killAllRandomly(Cluster cluster, int clusterSize) throws Exception {
    for (int size = clusterSize; size > 1; size--) {
      int toKill = random.nextInt(size);
      cluster.killNode(toKill);
    }
  }

  public void testSimpleRandomKill() throws Exception {
    int clusterSize = 10;
    Cluster cluster = new Cluster(clusterSize);
    cluster.createCluster();
    cluster.deployMods(clusterSize / 2, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    killAllRandomly(cluster, clusterSize);
    cluster.closeCluster();
  }

  public void testMultipleRandomKill() throws Exception {
    int clusterSize = 10;
    Cluster cluster = new Cluster(clusterSize);
    cluster.createCluster();
    cluster.deployMods(clusterSize / 2, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")).addDeployment(new DepInfo("io.vertx~ha-test2~1.0")));
    killAllRandomly(cluster, clusterSize);
    cluster.closeCluster();
  }

  public void testSimpleKillAndDeployMore() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.killNode(1);
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test2~1.0")));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testCloseThenKill() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.closeNode(0);
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testSimpleNodeConfigAndInstances() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0", 3, new JsonObject().putString("foo", "bar"))));
    cluster.killNode(0);
    cluster.closeCluster();
  }

  public void testSimpleNodeConfigAndInstancesSameModuleOtherNodes() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0", 6, new JsonObject().putString("foo", "bar"))));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0", 3, new JsonObject().putString("blah", "eek"))));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0", 7, new JsonObject().putString("foo", "quux"))));
    cluster.killNode(0);
    cluster.killNode(0);
    cluster.closeCluster();
  }

  public void testGroups() throws Exception {
    Cluster cluster = new Cluster();
    cluster.nodes.add(new NodeMods("group1"));
    cluster.nodes.add(new NodeMods("group1"));
    cluster.nodes.add(new NodeMods("group2"));
    cluster.nodes.add(new NodeMods("group2"));
    cluster.nodes.add(new NodeMods("group3"));
    cluster.nodes.add(new NodeMods("group3"));

    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.deployMods(3, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.deployMods(4, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));
    cluster.deployMods(5, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0")));

    assertEquals(0, cluster.killNode(0));
    cluster.closeNode(0);

    assertEquals(0, cluster.killNode(0));
    cluster.closeNode(0);

    assertEquals(0, cluster.killNode(0));
    cluster.closeNode(0);

    cluster.closeCluster();
  }

  public void testFailureDuringFailover() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.pms.get(1).failDuringFailover(true);
    cluster.pms.get(2).failDuringFailover(true);
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0", 6, new JsonObject().putString("foo", "bar"))));

    // -1 means failover failed
    assertEquals(-1, cluster.killNode(0));

    // Next time we won't fail during failover - the next failover should take over the previous failed node which
    // was in limbo
    cluster.pms.get(1).failDuringFailover(false);
    assertEquals(0, cluster.killNode(0));

    cluster.closeCluster();
  }

  public void testSimpleQuorum() throws Exception {
    Cluster cluster = new Cluster();
    cluster.nodes.add(new NodeMods("group1", 2));
    cluster.createCluster();

    NodeMods mods = new NodeMods().addDeployment(new DepInfo("io.vertx~ha-test1~1.0"));
    cluster.deployModsNoCheck(0, mods);

    Thread.sleep(500);
    // Make sure it doesn't deploy yet - we don't have a quorum
    assertTrue(cluster.pms.get(0).getDeployments().isEmpty());

    // Now deploy another node
    cluster.addNode(new NodeMods("group1", 2));

    Thread.sleep(500);

    // Should now be deployed
    cluster.checkModulesDeployed(0, mods);

    //cluster.nodes.add(new NodeMods("group1", 2));
  }

  class Cluster {
    List<NodeMods> nodes = new ArrayList<>();
    List<TestPlatformManager> pms;

    Cluster(int nodeCount) {
      for (int i = 0; i < nodeCount; i++) {
        nodes.add(new NodeMods());
      }
    }

    Cluster() {
    }

    void createCluster() {
      pms = new ArrayList<>();
      for (NodeMods node: nodes) {
        pms.add(new TestPlatformManager(0, "localhost", node.quorumSize, node.group));
      }
    }

    void addNode(NodeMods node) {
      nodes.add(node);
      pms.add(new TestPlatformManager(0, "localhost", node.quorumSize, node.group));
    }

    void deployMods(int node, NodeMods nodeMods) throws Exception {
      TestPlatformManager pm = pms.get(node);
      final CountDownLatch latch = new CountDownLatch(nodeMods.deployments.size());
      for (DepInfo dep: nodeMods.deployments) {
        pm.deployModule(dep.modName, dep.config, dep.instances, true, new Handler<AsyncResult<String>>() {
          @Override
          public void handle(AsyncResult<String> res) {
            if (res.succeeded()) {
              latch.countDown();
            } else {
              res.cause().printStackTrace();
              fail("Failed to deploy module");
            }
          }
        });
      }
      assertTrue(latch.await(10, TimeUnit.SECONDS));
      checkModulesDeployed(node, nodeMods);
    }

    void deployModsNoCheck(int node, NodeMods nodeMods) throws Exception {
      TestPlatformManager pm = pms.get(node);
      for (DepInfo dep: nodeMods.deployments) {
        pm.deployModule(dep.modName, dep.config, dep.instances, true, new Handler<AsyncResult<String>>() {
          @Override
          public void handle(AsyncResult<String> res) {
            if (!res.succeeded()) {
              res.cause().printStackTrace();
              fail("Failed to deploy module");
            }
          }
        });
      }
    }

    void checkModulesDeployed(int node, NodeMods nodeMods) throws Exception {
      NodeMods existingMods = nodes.get(node);
      existingMods.deployments.addAll(nodeMods.deployments);
      for (int i = 0; i < pms.size(); i++) {
        NodeMods mods = nodes.get(i);
        TestPlatformManager pm = pms.get(i);
        assertEquals(mods.deployments.size(), pm.getDeployments().size());
        for (DepInfo dep: mods.deployments) {
          assertTrue(hasModule(dep.modName, pm.getDeployments()));
        }
      }
    }

    void closeCluster() {
      for (TestPlatformManager pm: pms) {
        pm.stop();
      }
    }

    void closeNode(int node) {
      pms.get(node).stop();
      nodes.remove(node);
      pms.remove(node);
    }

    NodeMods limboMods;

    int killNode(int node) throws Exception {
      TestPlatformManager toKill = pms.get(node);
      NodeMods failoverMods = nodes.get(node);
      nodes.remove(node);
      pms.remove(node);

      final CountDownLatch failoverLatch = new CountDownLatch(1);
      final AtomicInteger afailoverNode = new AtomicInteger(-1);
      for (int i = 0; i < pms.size(); i++) {
        final int nodeID = i;
        pms.get(i).failoverCompleteHandler(new Handler<Boolean>() {
          @Override
          public void handle(Boolean b) {
            if (b) {
              afailoverNode.set(nodeID);
            }
            failoverLatch.countDown();
          }
        });
      }

      toKill.simulateKill();

      assertTrue(failoverLatch.await(10, TimeUnit.SECONDS));

      int failoverNode = afailoverNode.get();

      if (failoverNode != -1) {
        // Now make sure that the mods on nodes other than the kill node are still there
        for (int i = 0; i < pms.size(); i++) {
          Map<String, Deployment> deployments = pms.get(i).getDeployments();
          Collection<Deployment> deps = deployments.values();
          NodeMods expectedMods = nodes.get(i);
          if (i == failoverNode) {
            expectedMods.deployments.addAll(failoverMods.deployments);
            // Also add in any mods which are in limbo from a previous failed failover attempt
            if (limboMods != null) {
              expectedMods.deployments.addAll(limboMods.deployments);
              limboMods = null;
            }
          }
          for (DepInfo expectedDep: expectedMods.deployments) {
            assertTrue(containsDep(deps, expectedDep));
          }
        }
      } else {
        limboMods = failoverMods;
      }
      return failoverNode;
    }

    private boolean containsDep(Collection<Deployment> deps, DepInfo dep) {
      for (Deployment d: deps) {
        if (!d.modID.toString().equals(dep.modName))  continue;
        if (d.config == null && dep.config != null) continue;
        if (d.config != null && !d.config.equals(dep.config)) continue;
        if (d.instances != dep.instances) continue;
        return true;
      }
      return false;
    }

    private boolean hasModule(String moduleName, Map<String, Deployment> deployments) {
      for (Deployment dep: deployments.values()) {
        if (dep.modID.toString().equals(moduleName)) {
          return true;
        }
      }
      return false;
    }

  }

  class NodeMods {
    List<DepInfo> deployments = new ArrayList<>();
    String group;
    int quorumSize;

    NodeMods() {
      this(null);
    }

    NodeMods(String group) {
      this.group = group;
    }

    NodeMods(String group, int quorumSize) {
      this.group = group;
      this.quorumSize = quorumSize;
    }

    NodeMods addDeployment(DepInfo dep) {
      this.deployments.add(dep);
      return this;
    }
  }

  class DepInfo {
    String modName;
    int instances;
    JsonObject config;

    DepInfo(String modName, int instances, JsonObject config) {
      this.modName = modName;
      this.instances = instances;
      this.config = config;
    }

    DepInfo(String modName) {
      this(modName, 1, new JsonObject());
    }
  }

}
