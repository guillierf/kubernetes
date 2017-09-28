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
	"k8s.io/api/core/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/cloudprovider/providers/vsphere/vclib"
	"golang.org/x/net/context"
	"strings"
	"github.com/golang/glog"
)

// Stores info about the kubernetes node
type NodeInfo struct {
	vm       *vclib.VirtualMachine
	vcServer string
}

type NodeManager struct {
	// Maps the VC server to VSphereInstance
	vsphereInstanceMap map[string]*VSphereInstance
	// Maps node name to node info.
	nodeInfoMap map[string]*NodeInfo
	registeredNodes map[string]*v1.Node
}

func (nm *NodeManager) discoverNode(node *v1.Node) error {
	for vc, vsi := range nm.vsphereInstanceMap {
		glog.V(4).Infof("Discovering node %s in vsphere %s", node.Name, vc)
		// Create context
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := vsi.conn.Connect(ctx)
		if err != nil {
			return err
		}
		var datacenterObjs []*vclib.Datacenter
		if vsi.cfg.Datacenters == "" {
			datacenterObjs, err = vclib.GetAllDatacenters(ctx, vsi.conn)
			if err != nil {
				// TODO: Retry here?
				return err
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
					// TODO: Retry here?
					return err
				}
				datacenterObjs = append(datacenterObjs, datacenterObj)
			}
		}
		glog.V(4).Infof("Found %d datacenters in vc %s", len(datacenterObjs), vc)

		type VmSearchResult struct {
			vm *vclib.VirtualMachine
			datacenter *vclib.Datacenter
			err error
		}

		c1 := make(chan *VmSearchResult)
		nodeUUID := node.Status.NodeInfo.SystemUUID
		for _, datacenterObj := range datacenterObjs {
			glog.V(4).Infof("Finding node %s in vc=%s and datacenter=%s", node.Name, vc, datacenterObj.Name())
			go func(dc *vclib.Datacenter) {
				vm, err := dc.GetVMByUUID(ctx, nodeUUID)
				c1 <- &VmSearchResult{
					vm:         vm,
					err:        err,
					datacenter: datacenterObj,
				}
			}(datacenterObj)
		}

		for i := 0; i < len(datacenterObjs); i++ {
			res := <-c1
			if res.vm != nil {
				glog.V(4).Infof("Found node %s as vm=%+v in vc=%s and datacenter=%s",
					node.Name, res.vm, vc, res.datacenter.Name())
			} else {
				glog.V(4).Infof("Did not find node %s in vc=%s and datacenter=%s",
					node.Name, vc, res.datacenter.Name())
			}
		}
	}
	return nil
}

func (nm *NodeManager) RegisterNode(node *v1.Node) {
	nm.registeredNodes[node.ObjectMeta.Name] = node
	glog.V(4).Infof("Calling discoverNode")
	go nm.discoverNode(node)
	glog.V(4).Infof("Called discoverNode")
}

func (nm *NodeManager) RediscoverNode(nodeName k8stypes.NodeName) {

}

func (nm *NodeManager) UnregisterNode(node *v1.Node) error {
	return nil
}

func (nm *NodeManager) GetNodeInfo(nodeName k8stypes.NodeName) (*NodeInfo, error) {
	return nil, nil
}

func (nm *NodeManager) GetVSphereInstance(nodeName k8stypes.NodeName) (*VSphereInstance, error) {
	return nil, nil
}
