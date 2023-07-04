/**
 * Copyright (c) 2023 Yunshan Networks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cache

import (
	cloudmodel "github.com/deepflowio/deepflow/server/controller/cloud/model"
	"github.com/deepflowio/deepflow/server/controller/db/mysql"
	. "github.com/deepflowio/deepflow/server/controller/recorder/common"
)

func (b *DiffBaseDataSet) addProcess(dbItem *mysql.Process, seq int) {
	b.Process[dbItem.Lcuuid] = &Process{
		DiffBase: DiffBase{
			Sequence: seq,
			Lcuuid:   dbItem.Lcuuid,
		},
		Name:        dbItem.Name,
		OSAPPTags:   dbItem.OSAPPTags,
		ContainerID: dbItem.ContainerID,
	}
	b.GetLogFunc()(addDiffBase(RESOURCE_TYPE_PROCESS_EN, b.Process[dbItem.Lcuuid]))
}

func (b *DiffBaseDataSet) deleteProcess(lcuuid string) {
	delete(b.Process, lcuuid)
	log.Info(deleteDiffBase(RESOURCE_TYPE_PROCESS_EN, lcuuid))
}

type Process struct {
	DiffBase
	Name        string `json:"name"`
	OSAPPTags   string `json:"os_app_tags"`
	ContainerID string `json:"container_id"`
}

func (p *Process) Update(cloudItem *cloudmodel.Process) {
	p.Name = cloudItem.Name
	p.OSAPPTags = cloudItem.OSAPPTags
	p.ContainerID = cloudItem.ContainerID
	log.Info(updateDiffBase(RESOURCE_TYPE_PROCESS_EN, p))
}
