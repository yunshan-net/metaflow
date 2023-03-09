/*
 * Copyright (c) 2022 Yunshan Networks
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

package service

import (
	"fmt"
	"net"

	controllerCommon "github.com/deepflowio/deepflow/server/controller/common"
	"github.com/deepflowio/deepflow/server/profile/common"
	"github.com/deepflowio/deepflow/server/profile/config"
	logging "github.com/op/go-logging"
)

var log = logging.MustGetLogger("profile")

func Tracing(args *common.ProfileParams) (jsonData map[string]interface{}, debug map[string]interface{}, err error) {
	url := fmt.Sprintf("http://%s/v1/query/", net.JoinHostPort(config.Cfg.Querier.Host, fmt.Sprintf("%d", config.Cfg.Querier.Port)))
	body := map[string]interface{}{}
	body["db"] = args.DB
	body["sql"] = "select profile_location_str, profile_node_id, profile_parent_node_id from in_process group by profile_location_str, profile_node_id, profile_parent_node_id limit 10"
	resp, err := controllerCommon.CURLPerform("POST", url, body)
	if err != nil {
		log.Errorf("call querier failed: %s, %s", err.Error(), url)
	}
	if len(resp.Get("result").MustMap()) == 0 {
		log.Warningf("no data in curl response: %s", url)
	}
	for k, v := range resp.Get("result").MustMap() {
		if k == "columns" {
			for index, item := range v.([]interface{}) {
				if item.string
				formatStr, err := common.JsonFormat(item.(string))
				if err != nil {
					fmt.Println("format json str faild: " + err.Error())
					continue
				}
				fmt.Println(formatStr)
			}
		}
		for _, item := range v.([]interface{}) {
			formatStr, err := common.JsonFormat(item.(string))
			if err != nil {
				fmt.Println("format json str faild: " + err.Error())
				continue
			}
			fmt.Println(formatStr)
		}
	}
	log.Debug(url)
	log.Infof("%#v", resp)
	return jsonData, debug, err
}
