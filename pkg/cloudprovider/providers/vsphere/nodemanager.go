/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vsphere

import (
	"fmt"
	"github.com/golang/glog"
	"golang.org/x/net/context"
	"k8s.io/api/core/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/cloudprovider/providers/vsphere/vclib"
	"strings"
	"sync"
)

// Stores info about the kubernetes node
type NodeInfo struct {
	dataCenter *vclib.Datacenter
	vm         *vclib.VirtualMachine
	vcServer   string
}

type NodeManager struct {
	// TODO: replace map with concurrent map when k8s supports go v1.9

	// Maps the VC server to VSphereInstance
	vsphereInstanceMap map[string]*VSphereInstance
	// Maps node name to node info.
	nodeInfoMap map[string]*NodeInfo
	// Maps node name to node structure
	registeredNodes map[string]*v1.Node

	// Mutexes
	registeredNodesLock sync.RWMutex
	nodeInfoLock        sync.RWMutex
}

type NodeDetails struct {
	NodeName string
	vm       *vclib.VirtualMachine
}

// TODO: Make it configurable in vsphere.conf
const (
	POOL_SIZE  = 8
	QUEUE_SIZE = POOL_SIZE * 10
)

func (nm *NodeManager) DiscoverNode(node *v1.Node) error {

	type VmSearch struct {
		vc         string
		datacenter *vclib.Datacenter
	}

	var mutex = &sync.Mutex{}

	var queueChannel chan *VmSearch
	var wg sync.WaitGroup
	queueChannel = make(chan *VmSearch, QUEUE_SIZE)
	nodeUUID := node.Status.NodeInfo.SystemUUID
	vmFound := false

	go func() {
		var datacenterObjs []*vclib.Datacenter
		for vc, vsi := range nm.vsphereInstanceMap {

			mutex.Lock()
			found := vmFound
			mutex.Unlock()
			if found == true {
				break
			}

			// Create context
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			err := vsi.conn.Connect(ctx)
			if err != nil {
				glog.V(4).Info("Discovering node error vc:", err)
				continue
			}

			if vsi.cfg.Datacenters == "" {
				datacenterObjs, err = vclib.GetAllDatacenter(ctx, vsi.conn)
				if err != nil {
					glog.V(4).Info("Discovering node error dc:", err)
					continue
				}
			} else {
				datacenters := strings.Split(vsi.cfg.Datacenters, ",")
				for _, dc := range datacenters {
					dc = strings.TrimSpace(dc)
					if dc == "" {
						continue
					}
					datacenterObj, err := vclib.GetDatacenter(ctx, vsi.conn, dc)
					if err != nil {
						glog.V(4).Info("Discovering node error dc:", err)
					}
					datacenterObjs = append(datacenterObjs, datacenterObj)
				}
			}

			for _, datacenterObj := range datacenterObjs {
				mutex.Lock()
				found := vmFound
				mutex.Unlock()
				if found == true {
					break
				}

				glog.V(4).Infof("Finding node %s in vc=%s and datacenter=%s", node.Name, vc, datacenterObj.Name())
				queueChannel <- &VmSearch{
					vc:         vc,
					datacenter: datacenterObj,
				}
			}
		}
		close(queueChannel)
	}()

	for i := 0; i < POOL_SIZE; i++ {
		go func() {
			for res := range queueChannel {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				vm, err := res.datacenter.GetVMByUUID(ctx, nodeUUID)
				if err != nil {
					glog.V(4).Infof("Error %q while looking for vm=%+v in vc=%s and datacenter=%s",
						err, node.Name, vm, res.vc, res.datacenter.Name())
					continue
				}
				if vm != nil {
					glog.V(4).Infof("Found node %s as vm=%+v in vc=%s and datacenter=%s",
						node.Name, vm, res.vc, res.datacenter.Name())

					nodeInfo := &NodeInfo{dataCenter: res.datacenter, vm: vm, vcServer: res.vc}
					nm.addNodeInfo(node.ObjectMeta.Name, nodeInfo)
					for range queueChannel {
					}
					mutex.Lock()
					vmFound = true
					mutex.Unlock()
					break

				} else {
					glog.V(4).Infof("Did not find node %s in vc=%s and datacenter=%s",
						node.Name, res.vc, res.datacenter.Name(), err)
				}
			}
			wg.Done()
		}()
		wg.Add(1)
	}
	wg.Wait()
	if !vmFound {
		return fmt.Errorf("discovery failed for node %q", node.ObjectMeta.Name)
	}
	return nil
}

func (nm *NodeManager) RegisterNode(node *v1.Node) error {
	nm.addNode(node)
	return nm.DiscoverNode(node)
}

func (nm *NodeManager) UnRegisterNode(node *v1.Node) error {
	nm.removeNode(node)
	return nil
}

func (nm *NodeManager) RediscoverNode(nodeName k8stypes.NodeName) error {
	node, err := nm.GetNode(nodeName)

	if err != nil {
		return err
	}
	return nm.DiscoverNode(&node)
}

func (nm *NodeManager) GetNode(nodeName k8stypes.NodeName) (v1.Node, error) {
	nm.registeredNodesLock.RLock()
	node := nm.registeredNodes[convertToString(nodeName)]
	nm.registeredNodesLock.RUnlock()
	if node == nil {
		return v1.Node{}, fmt.Errorf("node %q not found", convertToString(nodeName))
	}
	return *node, nil
}

func (nm *NodeManager) addNode(node *v1.Node) {
	nm.registeredNodesLock.Lock()
	nm.registeredNodes[node.ObjectMeta.Name] = node
	nm.registeredNodesLock.Unlock()
}

func (nm *NodeManager) removeNode(node *v1.Node) {
	nm.registeredNodesLock.Lock()
	delete(nm.registeredNodes, node.ObjectMeta.Name)
	nm.registeredNodesLock.Unlock()
}

func (nm *NodeManager) GetNodeInfo(nodeName k8stypes.NodeName) (NodeInfo, error) {
	nm.nodeInfoLock.RLock()
	nodeInfo := nm.nodeInfoMap[convertToString(nodeName)]
	nm.nodeInfoLock.RUnlock()
	if nodeInfo == nil {
		err := nm.RediscoverNode(nodeName)
		if err != nil {
			return NodeInfo{}, fmt.Errorf("error %q node info for node %q not found", err, convertToString(nodeName))
		}
		nm.nodeInfoLock.RLock()
		nodeInfo = nm.nodeInfoMap[convertToString(nodeName)]
		nm.nodeInfoLock.RUnlock()
	}
	return *nodeInfo, nil
}

func (nm *NodeManager) GetNodeDetails() []NodeDetails {
	nm.nodeInfoLock.RLock()
	defer nm.nodeInfoLock.RUnlock()
	var nodeDetails []NodeDetails
	for nodeName, nodeInfo := range nm.nodeInfoMap {
		nodeDetails = append(nodeDetails, NodeDetails{nodeName, nodeInfo.vm})
	}
	return nodeDetails
}

func (nm *NodeManager) addNodeInfo(nodeName string, nodeInfo *NodeInfo) {
	nm.nodeInfoLock.Lock()
	nm.nodeInfoMap[nodeName] = nodeInfo
	nm.nodeInfoLock.Unlock()
}

func (nm *NodeManager) GetVSphereInstance(nodeName k8stypes.NodeName) (VSphereInstance, error) {
	nodeInfo, err := nm.GetNodeInfo(nodeName)
	if err != nil {
		return VSphereInstance{}, fmt.Errorf("node info for node %q not found", convertToString(nodeName))
	}
	vsphereInstance := nm.vsphereInstanceMap[nodeInfo.vcServer]
	if vsphereInstance == nil {
		return VSphereInstance{}, fmt.Errorf("vSphereInstance for vc server %q not found while looking for node %q", nodeInfo.vcServer, convertToString(nodeName))
	}
	return *vsphereInstance, nil
}
