// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	maxDownTime := cluster.GetMaxStoreDownTime()

	// Get all suitable stores
	stores := []*core.StoreInfo{}
	for _, v := range cluster.GetStores() {
		if v.IsUp() && v.DownTime() <= maxDownTime {
			stores = append(stores, v)
		}
	}
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() > stores[j].GetRegionSize()
	})
	if len(stores) == 0 {
		return nil
	}

	// Find a region from suitable fromStore
	var oldStore, newStore *core.StoreInfo
	var region *core.RegionInfo
	for _, s := range stores {
		sel := func(rc core.RegionsContainer) {
			region = rc.RandomRegion(nil, nil)
		}
		cluster.GetPendingRegionsWithLock(s.GetID(), sel)
		if region != nil {
			oldStore = s
			break
		}
		cluster.GetFollowersWithLock(s.GetID(), sel)
		if region != nil {
			oldStore = s
			break
		}
		cluster.GetLeadersWithLock(s.GetID(), sel)
		if region != nil {
			oldStore = s
			break
		}
	}
	if region == nil {
		return nil
	}

	// Get all stores that contain this region
	storeIds := region.GetStoreIds()
	if len(storeIds) < cluster.GetMaxReplicas() {
		return nil
	}

	// Find a newStore
	for i := len(stores) - 1; i >= 0 && stores[i] != oldStore; i-- {
		if _, ok := storeIds[stores[i].GetID()]; !ok {
			newStore = stores[i]
			break
		}
	}

	if newStore == nil {
		return nil
	}

	// If this is valuable
	if oldStore.GetRegionSize()-newStore.GetRegionSize() <= 2*region.GetApproximateSize() {
		return nil
	}

	peer, err := cluster.AllocPeer(newStore.GetID())
	if err != nil {
		return nil
	}

	op, err := operator.CreateMovePeerOperator("move peer", cluster, region, operator.OpBalance, oldStore.GetID(), newStore.GetID(), peer.Id)
	if err != nil {
		return nil
	}

	return op
}
