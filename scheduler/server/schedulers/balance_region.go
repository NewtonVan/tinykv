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
	"fmt"
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
	return s.opController.OperatorCount(operator.OpRegion) > cluster.GetRegionScheduleLimit()
}

type storeSlice []*core.StoreInfo

func (s storeSlice) Len() int {
	return len(s)
}

func (s storeSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s storeSlice) Less(i, j int) bool {
	return s[i].GetRegionSize() < s[j].GetRegionSize()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// 1. find suitable store
	suitableStores := make(storeSlice, 0)
	maxStoreDownTime := cluster.GetMaxStoreDownTime()
	for _, storeInfo := range cluster.GetStores() {
		if storeInfo.IsUp() && storeInfo.DownTime() <= maxStoreDownTime {
			suitableStores = append(suitableStores, storeInfo)
		}
	}
	if len(suitableStores) < 2 {
		return nil
	}

	// 2. find store and region
	sort.Sort(suitableStores)
	var fromStore, toStore *core.StoreInfo
	var targetRegion *core.RegionInfo
	for i := len(suitableStores) - 1; i >= 0; i-- {
		var targetRc core.RegionsContainer
		cluster.GetPendingRegionsWithLock(suitableStores[i].GetID(), func(rc core.RegionsContainer) {
			targetRc = rc
		})
		targetRegion = targetRc.RandomRegion(nil, nil)
		if targetRegion != nil {
			fromStore = suitableStores[i]
			break
		}

		cluster.GetFollowersWithLock(suitableStores[i].GetID(), func(rc core.RegionsContainer) {
			targetRc = rc
		})
		targetRegion = targetRc.RandomRegion(nil, nil)
		if targetRegion != nil {
			fromStore = suitableStores[i]
			break
		}

		cluster.GetLeadersWithLock(suitableStores[i].GetID(), func(rc core.RegionsContainer) {
			targetRc = rc
		})
		targetRegion = targetRc.RandomRegion(nil, nil)
		if targetRegion != nil {
			fromStore = suitableStores[i]
			break
		}
	}
	if targetRegion == nil {
		return nil
	}

	// 2.2 find target store
	storeIds := targetRegion.GetStoreIds()
	if len(storeIds) < cluster.GetMaxReplicas() {
		return nil
	}

	for i := range suitableStores {
		if _, ok := storeIds[suitableStores[i].GetID()]; !ok {
			toStore = suitableStores[i]
			break
		}
	}
	if toStore == nil {
		return nil
	}
	if fromStore.GetRegionSize()-toStore.GetRegionSize() <= 2*targetRegion.GetApproximateSize() {
		return nil
	}

	// 3. create move op
	newPeer, err := cluster.AllocPeer(toStore.GetID())
	if err != nil {
		return nil
	}
	desc := fmt.Sprintf("move-from-%d-to-%d", fromStore.GetID(), toStore.GetID())

	op, err := operator.CreateMovePeerOperator(desc, cluster, targetRegion, operator.OpBalance, fromStore.GetID(), toStore.GetID(), newPeer.GetId())
	if err != nil {
		return nil
	}

	return op
}
