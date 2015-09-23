/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.nodelabels;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.nodelabels.event.StoreNewClusterNodeLabels;
import org.apache.hadoop.yarn.nodelabels.event.NodeLabelsStoreEvent;
import org.apache.hadoop.yarn.nodelabels.event.NodeLabelsStoreEventType;
import org.apache.hadoop.yarn.nodelabels.event.RemoveClusterNodeLabels;
import org.apache.hadoop.yarn.nodelabels.event.UpdateNodeToLabelsMappingsEvent;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.collect.ImmutableSet;

public class CommonNodeLabelsManager extends AbstractService {
  protected static final Log LOG = LogFactory.getLog(CommonNodeLabelsManager.class);
  private static final int MAX_LABEL_LENGTH = 255;
  public static final Set<String> EMPTY_STRING_SET = Collections
      .unmodifiableSet(new HashSet<String>(0));
  public static final String ANY = "*";
  public static final Set<String> ACCESS_ANY_LABEL_SET = ImmutableSet.of(ANY);
  private static final Pattern LABEL_PATTERN = Pattern
      .compile("^[0-9a-zA-Z][0-9a-zA-Z-_]*");
  public static final int WILDCARD_PORT = 0;

  /**
   * If a user doesn't specify label of a queue or node, it belongs
   * DEFAULT_LABEL
   */
  public static final String NO_LABEL = "";

  protected Dispatcher dispatcher;

  protected ConcurrentMap<String, Label> labelCollections =
      new ConcurrentHashMap<String, Label>();
  protected ConcurrentMap<String, Host> nodeCollections =
      new ConcurrentHashMap<String, Host>();

  protected final ReadLock readLock;
  protected final WriteLock writeLock;

  protected NodeLabelsStore store;

  protected static class Label {
    public Resource resource;

    protected Label() {
      this.resource = Resource.newInstance(0, 0);
    }
  }

  /**
   * A <code>Host</code> can have multiple <code>Node</code>s 
   */
  protected static class Host {
    public Set<String> labels;
    public Map<NodeId, Node> nms;
    
    protected Host() {
      labels =
          Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
      nms = new ConcurrentHashMap<NodeId, Node>();
    }
    
    public Host copy() {
      Host c = new Host();
      c.labels = new HashSet<String>(labels);
      for (Entry<NodeId, Node> entry : nms.entrySet()) {
        c.nms.put(entry.getKey(), entry.getValue().copy());
      }
      return c;
    }
  }
  
  protected static class Node {
    public Set<String> labels;
    public Resource resource;
    public boolean running;
    
    protected Node() {
      labels = null;
      resource = Resource.newInstance(0, 0);
      running = false;
    }
    
    public Node copy() {
      Node c = new Node();
      if (labels != null) {
        c.labels =
            Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        c.labels.addAll(labels);
      } else {
        c.labels = null;
      }
      c.resource = Resources.clone(resource);
      c.running = running;
      return c;
    }
  }

  private final class ForwardingEventHandler implements
      EventHandler<NodeLabelsStoreEvent> {

    @Override
    public void handle(NodeLabelsStoreEvent event) {
      handleStoreEvent(event);
    }
  }
  
  // Dispatcher related code
  protected void handleStoreEvent(NodeLabelsStoreEvent event) {
    try {
      switch (event.getType()) {
      case ADD_LABELS:
        StoreNewClusterNodeLabels storeNewClusterNodeLabelsEvent =
            (StoreNewClusterNodeLabels) event;
        store.storeNewClusterNodeLabels(storeNewClusterNodeLabelsEvent
             .getLabels());
        break;
      case REMOVE_LABELS:
        RemoveClusterNodeLabels removeClusterNodeLabelsEvent =
            (RemoveClusterNodeLabels) event;
        store.removeClusterNodeLabels(removeClusterNodeLabelsEvent.getLabels());
        break;
      case STORE_NODE_TO_LABELS:
        UpdateNodeToLabelsMappingsEvent updateNodeToLabelsMappingsEvent =
            (UpdateNodeToLabelsMappingsEvent) event;
        store.updateNodeToLabelsMappings(updateNodeToLabelsMappingsEvent
            .getNodeToLabels());
        break;
      }
    } catch (IOException e) {
      LOG.error("Failed to store label modification to storage");
      throw new YarnRuntimeException(e);
    }
  }

  public CommonNodeLabelsManager() {
    super(CommonNodeLabelsManager.class.getName());
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  // for UT purpose
  protected void initDispatcher(Configuration conf) {
    // create async handler
    dispatcher = new AsyncDispatcher();
    AsyncDispatcher asyncDispatcher = (AsyncDispatcher) dispatcher;
    asyncDispatcher.init(conf);
    asyncDispatcher.setDrainEventsOnStop();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    initNodeLabelStore(conf);
    
    labelCollections.put(NO_LABEL, new Label());
  }

  protected void initNodeLabelStore(Configuration conf) throws Exception {
    this.store = new FileSystemNodeLabelsStore(this);
    this.store.init(conf);
    this.store.recover();
  }

  // for UT purpose
  protected void startDispatcher() {
    // start dispatcher
    AsyncDispatcher asyncDispatcher = (AsyncDispatcher) dispatcher;
    asyncDispatcher.start();
  }

  @Override
  protected void serviceStart() throws Exception {
    // init dispatcher only when service start, because recover will happen in
    // service init, we don't want to trigger any event handling at that time.
    initDispatcher(getConfig());

    if (null != dispatcher) {
      dispatcher.register(NodeLabelsStoreEventType.class,
          new ForwardingEventHandler());
    }
    
    startDispatcher();
  }
  
  // for UT purpose
  protected void stopDispatcher() {
    AsyncDispatcher asyncDispatcher = (AsyncDispatcher) dispatcher;
    asyncDispatcher.stop();
  }
  
  @Override
  protected void serviceStop() throws Exception {
    // finalize store
    stopDispatcher();
    
    // only close store when we enabled store persistent
    if (null != store) {
      store.close();
    }
  }

  /**
   * Add multiple node labels to repository
   * 
   * @param labels
   *          new node labels added
   */
  @SuppressWarnings("unchecked")
  public void addToCluserNodeLabels(Set<String> labels) throws IOException {
    if (null == labels || labels.isEmpty()) {
      return;
    }
    Set<String> newLabels = new HashSet<String>();
    labels = normalizeLabels(labels);

    // do a check before actual adding them, will throw exception if any of them
    // doesn't meet label name requirement
    for (String label : labels) {
      checkAndThrowLabelName(label);
    }

    for (String label : labels) {
      // shouldn't overwrite it to avoid changing the Label.resource
      if (this.labelCollections.get(label) == null) {
        this.labelCollections.put(label, new Label());
        newLabels.add(label);
      }
    }
    if (null != dispatcher && !newLabels.isEmpty()) {
      dispatcher.getEventHandler().handle(
          new StoreNewClusterNodeLabels(newLabels));
    }

    LOG.info("Add labels: [" + StringUtils.join(labels.iterator(), ",") + "]");
  }
  
  protected void checkAddLabelsToNode(
      Map<NodeId, Set<String>> addedLabelsToNode) throws IOException {
    if (null == addedLabelsToNode || addedLabelsToNode.isEmpty()) {
      return;
    }

    // check all labels being added existed
    Set<String> knownLabels = labelCollections.keySet();
    for (Entry<NodeId, Set<String>> entry : addedLabelsToNode.entrySet()) {
      if (!knownLabels.containsAll(entry.getValue())) {
        String msg =
            "Not all labels being added contained by known "
                + "label collections, please check" + ", added labels=["
                + StringUtils.join(entry.getValue(), ",") + "]";
        LOG.error(msg);
        throw new IOException(msg);
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  protected void internalAddLabelsToNode(
      Map<NodeId, Set<String>> addedLabelsToNode) throws IOException {
    // do add labels to nodes
    Map<NodeId, Set<String>> newNMToLabels =
        new HashMap<NodeId, Set<String>>();
    for (Entry<NodeId, Set<String>> entry : addedLabelsToNode.entrySet()) {
      NodeId nodeId = entry.getKey();
      Set<String> labels = entry.getValue();
 
      createHostIfNonExisted(nodeId.getHost());
      if (nodeId.getPort() == WILDCARD_PORT) {
        Host host = nodeCollections.get(nodeId.getHost());
        host.labels.addAll(labels);
        newNMToLabels.put(nodeId, host.labels);
      } else {
        createNodeIfNonExisted(nodeId);
        Node nm = getNMInNodeSet(nodeId);
        if (nm.labels == null) {
          nm.labels = new HashSet<String>();
        }
        nm.labels.addAll(labels);
        newNMToLabels.put(nodeId, nm.labels);
      }
    }

    if (null != dispatcher) {
      dispatcher.getEventHandler().handle(
          new UpdateNodeToLabelsMappingsEvent(newNMToLabels));
    }

    // shows node->labels we added
    LOG.info("addLabelsToNode:");
    for (Entry<NodeId, Set<String>> entry : newNMToLabels.entrySet()) {
      LOG.info("  NM=" + entry.getKey() + ", labels=["
          + StringUtils.join(entry.getValue().iterator(), ",") + "]");
    }
  }
  
  /**
   * add more labels to nodes
   * 
   * @param addedLabelsToNode node -> labels map
   */
  public void addLabelsToNode(Map<NodeId, Set<String>> addedLabelsToNode)
      throws IOException {
    addedLabelsToNode = normalizeNodeIdToLabels(addedLabelsToNode);
    checkAddLabelsToNode(addedLabelsToNode);
    internalAddLabelsToNode(addedLabelsToNode);
  }
  
  protected void checkRemoveFromClusterNodeLabels(
      Collection<String> labelsToRemove) throws IOException {
    if (null == labelsToRemove || labelsToRemove.isEmpty()) {
      return;
    }

    // Check if label to remove doesn't existed or null/empty, will throw
    // exception if any of labels to remove doesn't meet requirement
    for (String label : labelsToRemove) {
      label = normalizeLabel(label);
      if (label == null || label.isEmpty()) {
        throw new IOException("Label to be removed is null or empty");
      }
      
      if (!labelCollections.containsKey(label)) {
        throw new IOException("Node label=" + label
            + " to be removed doesn't existed in cluster "
            + "node labels collection.");
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  protected void internalRemoveFromClusterNodeLabels(Collection<String> labelsToRemove) {
    // remove labels from nodes
    for (String nodeName : nodeCollections.keySet()) {
      Host host = nodeCollections.get(nodeName);
      if (null != host) {
        host.labels.removeAll(labelsToRemove);
        for (Node nm : host.nms.values()) {
          if (nm.labels != null) {
            nm.labels.removeAll(labelsToRemove);
          }
        }
      }
    }

    // remove labels from node labels collection
    for (String label : labelsToRemove) {
      labelCollections.remove(label);
    }

    // create event to remove labels
    if (null != dispatcher) {
      dispatcher.getEventHandler().handle(
          new RemoveClusterNodeLabels(labelsToRemove));
    }

    LOG.info("Remove labels: ["
        + StringUtils.join(labelsToRemove.iterator(), ",") + "]");
  }

  /**
   * Remove multiple node labels from repository
   * 
   * @param labelsToRemove
   *          node labels to remove
   * @throws IOException
   */
  public void removeFromClusterNodeLabels(Collection<String> labelsToRemove)
      throws IOException {
    labelsToRemove = normalizeLabels(labelsToRemove);
    
    checkRemoveFromClusterNodeLabels(labelsToRemove);

    internalRemoveFromClusterNodeLabels(labelsToRemove);
  }
  
  protected void checkRemoveLabelsFromNode(
      Map<NodeId, Set<String>> removeLabelsFromNode) throws IOException {
    // check all labels being added existed
    Set<String> knownLabels = labelCollections.keySet();
    for (Entry<NodeId, Set<String>> entry : removeLabelsFromNode.entrySet()) {
      NodeId nodeId = entry.getKey();
      Set<String> labels = entry.getValue();
      
      if (!knownLabels.containsAll(labels)) {
        String msg =
            "Not all labels being removed contained by known "
                + "label collections, please check" + ", removed labels=["
                + StringUtils.join(labels, ",") + "]";
        LOG.error(msg);
        throw new IOException(msg);
      }
      
      Set<String> originalLabels = null;
      
      boolean nodeExisted = false;
      if (WILDCARD_PORT != nodeId.getPort()) {
        Node nm = getNMInNodeSet(nodeId);
        if (nm != null) {
          originalLabels = nm.labels;
          nodeExisted = true;
        }
      } else {
        Host host = nodeCollections.get(nodeId.getHost());
        if (null != host) {
          originalLabels = host.labels;
          nodeExisted = true;
        }
      }
      
      if (!nodeExisted) {
        String msg =
            "Try to remove labels from NM=" + nodeId
                + ", but the NM doesn't existed";
        LOG.error(msg);
        throw new IOException(msg);
      }

      // the labels will never be null
      if (labels.isEmpty()) {
        continue;
      }

      // originalLabels may be null,
      // because when a Node is created, Node.labels can be null.
      if (originalLabels == null || !originalLabels.containsAll(labels)) {
        String msg =
            "Try to remove labels = [" + StringUtils.join(labels, ",")
                + "], but not all labels contained by NM=" + nodeId;
        LOG.error(msg);
        throw new IOException(msg);
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  protected void internalRemoveLabelsFromNode(
      Map<NodeId, Set<String>> removeLabelsFromNode) {
    // do remove labels from nodes
    Map<NodeId, Set<String>> newNMToLabels =
        new HashMap<NodeId, Set<String>>();
    for (Entry<NodeId, Set<String>> entry : removeLabelsFromNode.entrySet()) {
      NodeId nodeId = entry.getKey();
      Set<String> labels = entry.getValue();
      
      if (nodeId.getPort() == WILDCARD_PORT) {
        Host host = nodeCollections.get(nodeId.getHost());
        host.labels.removeAll(labels);
        newNMToLabels.put(nodeId, host.labels);
      } else {
        Node nm = getNMInNodeSet(nodeId);
        if (nm.labels != null) {
          nm.labels.removeAll(labels);
          newNMToLabels.put(nodeId, nm.labels);
        }
      }
    }
    
    if (null != dispatcher) {
      dispatcher.getEventHandler().handle(
          new UpdateNodeToLabelsMappingsEvent(newNMToLabels));
    }

    // shows node->labels we added
    LOG.info("removeLabelsFromNode:");
    for (Entry<NodeId, Set<String>> entry : newNMToLabels.entrySet()) {
      LOG.info("  NM=" + entry.getKey() + ", labels=["
          + StringUtils.join(entry.getValue().iterator(), ",") + "]");
    }
  }
  
  /**
   * remove labels from nodes, labels being removed most be contained by these
   * nodes
   * 
   * @param removeLabelsFromNode node -> labels map
   */
  public void
      removeLabelsFromNode(Map<NodeId, Set<String>> removeLabelsFromNode)
          throws IOException {
    removeLabelsFromNode = normalizeNodeIdToLabels(removeLabelsFromNode);
    
    checkRemoveLabelsFromNode(removeLabelsFromNode);

    internalRemoveLabelsFromNode(removeLabelsFromNode);
  }
  
  protected void checkReplaceLabelsOnNode(
      Map<NodeId, Set<String>> replaceLabelsToNode) throws IOException {
    if (null == replaceLabelsToNode || replaceLabelsToNode.isEmpty()) {
      return;
    }
    
    // check all labels being added existed
    Set<String> knownLabels = labelCollections.keySet();
    for (Entry<NodeId, Set<String>> entry : replaceLabelsToNode.entrySet()) {
      if (!knownLabels.containsAll(entry.getValue())) {
        String msg =
            "Not all labels being replaced contained by known "
                + "label collections, please check" + ", new labels=["
                + StringUtils.join(entry.getValue(), ",") + "]";
        LOG.error(msg);
        throw new IOException(msg);
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  protected void internalReplaceLabelsOnNode(
      Map<NodeId, Set<String>> replaceLabelsToNode) throws IOException {
    // do replace labels to nodes
    Map<NodeId, Set<String>> newNMToLabels = new HashMap<NodeId, Set<String>>();
    for (Entry<NodeId, Set<String>> entry : replaceLabelsToNode.entrySet()) {
      NodeId nodeId = entry.getKey();
      Set<String> labels = entry.getValue();

      createHostIfNonExisted(nodeId.getHost());      
      if (nodeId.getPort() == WILDCARD_PORT) {
        Host host = nodeCollections.get(nodeId.getHost());
        host.labels.clear();
        host.labels.addAll(labels);
        newNMToLabels.put(nodeId, host.labels);
      } else {
        createNodeIfNonExisted(nodeId);
        Node nm = getNMInNodeSet(nodeId);
        if (nm.labels == null) {
          nm.labels = new HashSet<String>();
        }
        nm.labels.clear();
        nm.labels.addAll(labels);
        newNMToLabels.put(nodeId, nm.labels);
      }
    }

    if (null != dispatcher) {
      dispatcher.getEventHandler().handle(
          new UpdateNodeToLabelsMappingsEvent(newNMToLabels));
    }

    // shows node->labels we added
    LOG.info("setLabelsToNode:");
    for (Entry<NodeId, Set<String>> entry : newNMToLabels.entrySet()) {
      LOG.info("  NM=" + entry.getKey() + ", labels=["
          + StringUtils.join(entry.getValue().iterator(), ",") + "]");
    }
  }
  
  /**
   * replace labels to nodes
   * 
   * @param replaceLabelsToNode node -> labels map
   */
  public void replaceLabelsOnNode(Map<NodeId, Set<String>> replaceLabelsToNode)
      throws IOException {
    replaceLabelsToNode = normalizeNodeIdToLabels(replaceLabelsToNode);
    
    checkReplaceLabelsOnNode(replaceLabelsToNode);

    internalReplaceLabelsOnNode(replaceLabelsToNode);
  }

  /**
   * Get mapping of nodes to labels
   * 
   * @return nodes to labels map
   */
  public Map<NodeId, Set<String>> getNodeLabels() {
    try {
      readLock.lock();
      Map<NodeId, Set<String>> nodeToLabels =
          new HashMap<NodeId, Set<String>>();
      for (Entry<String, Host> entry : nodeCollections.entrySet()) {
        String hostName = entry.getKey();
        Host host = entry.getValue();
        for (NodeId nodeId : host.nms.keySet()) {
          Set<String> nodeLabels = getLabelsByNode(nodeId);
          if (nodeLabels == null || nodeLabels.isEmpty()) {
            continue;
          }
          nodeToLabels.put(nodeId, nodeLabels);
        }
        if (!host.labels.isEmpty()) {
          nodeToLabels
              .put(NodeId.newInstance(hostName, WILDCARD_PORT), host.labels);
        }
      }
      return Collections.unmodifiableMap(nodeToLabels);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get existing valid labels in repository
   * 
   * @return existing valid labels in repository
   */
  public Set<String> getClusterNodeLabels() {
    try {
      readLock.lock();
      Set<String> labels = new HashSet<String>(labelCollections.keySet());
      labels.remove(NO_LABEL);
      return Collections.unmodifiableSet(labels);
    } finally {
      readLock.unlock();
    }
  }

  private void checkAndThrowLabelName(String label) throws IOException {
    if (label == null || label.isEmpty() || label.length() > MAX_LABEL_LENGTH) {
      throw new IOException("label added is empty or exceeds "
          + MAX_LABEL_LENGTH + " character(s)");
    }
    label = label.trim();

    boolean match = LABEL_PATTERN.matcher(label).matches();

    if (!match) {
      throw new IOException("label name should only contains "
          + "{0-9, a-z, A-Z, -, _} and should not started with {-,_}"
          + ", now it is=" + label);
    }
  }

  protected String normalizeLabel(String label) {
    if (label != null) {
      return label.trim();
    }
    return NO_LABEL;
  }

  private Set<String> normalizeLabels(Collection<String> labels) {
    Set<String> newLabels = new HashSet<String>();
    for (String label : labels) {
      newLabels.add(normalizeLabel(label));
    }
    return newLabels;
  }
  
  protected Node getNMInNodeSet(NodeId nodeId) {
    return getNMInNodeSet(nodeId, nodeCollections);
  }
  
  protected Node getNMInNodeSet(NodeId nodeId, Map<String, Host> map) {
    return getNMInNodeSet(nodeId, map, false);
  }

  protected Node getNMInNodeSet(NodeId nodeId, Map<String, Host> map,
      boolean checkRunning) {
    Host host = map.get(nodeId.getHost());
    if (null == host) {
      return null;
    }
    Node nm = host.nms.get(nodeId);
    if (null == nm) {
      return null;
    }
    if (checkRunning) {
      return nm.running ? nm : null; 
    }
    return nm;
  }
  
  protected Set<String> getLabelsByNode(NodeId nodeId) {
    return getLabelsByNode(nodeId, nodeCollections);
  }
  
  protected Set<String> getLabelsByNode(NodeId nodeId, Map<String, Host> map) {
    Host host = map.get(nodeId.getHost());
    if (null == host) {
      return EMPTY_STRING_SET;
    }
    Node nm = host.nms.get(nodeId);
    if (null != nm && null != nm.labels) {
      return nm.labels;
    } else {
      return host.labels;
    }
  }
  
  protected void createNodeIfNonExisted(NodeId nodeId) throws IOException {
    Host host = nodeCollections.get(nodeId.getHost());
    if (null == host) {
      throw new IOException("Should create host before creating node.");
    }
    Node nm = host.nms.get(nodeId);
    if (null == nm) {
      host.nms.put(nodeId, new Node());
    }
  }
  
  protected void createHostIfNonExisted(String hostName) {
    Host host = nodeCollections.get(hostName);
    if (null == host) {
      host = new Host();
      nodeCollections.put(hostName, host);
    }
  }
  
  protected Map<NodeId, Set<String>> normalizeNodeIdToLabels(
      Map<NodeId, Set<String>> nodeIdToLabels) {
    Map<NodeId, Set<String>> newMap = new HashMap<NodeId, Set<String>>();
    for (Entry<NodeId, Set<String>> entry : nodeIdToLabels.entrySet()) {
      NodeId id = entry.getKey();
      Set<String> labels = entry.getValue();
      newMap.put(id, normalizeLabels(labels)); 
    }
    return newMap;
  }
}
