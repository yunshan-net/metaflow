/*
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

package controller

import (
	"context"
	"flag"
	"os"
	"strconv"
	"time"

	logging "github.com/op/go-logging"
	yaml "gopkg.in/yaml.v2"

	servercommon "github.com/deepflowio/deepflow/server/common"
	"github.com/deepflowio/deepflow/server/controller/common"
	"github.com/deepflowio/deepflow/server/controller/config"
	"github.com/deepflowio/deepflow/server/controller/db/mysql"
	"github.com/deepflowio/deepflow/server/controller/db/redis"
	"github.com/deepflowio/deepflow/server/controller/election"
	"github.com/deepflowio/deepflow/server/controller/genesis"
	"github.com/deepflowio/deepflow/server/controller/grpc"
	"github.com/deepflowio/deepflow/server/controller/http"
	"github.com/deepflowio/deepflow/server/controller/http/router"
	"github.com/deepflowio/deepflow/server/controller/manager"
	"github.com/deepflowio/deepflow/server/controller/monitor"
	"github.com/deepflowio/deepflow/server/controller/prometheus"
	recorderdb "github.com/deepflowio/deepflow/server/controller/recorder/db"
	"github.com/deepflowio/deepflow/server/controller/report"
	"github.com/deepflowio/deepflow/server/controller/statsd"
	"github.com/deepflowio/deepflow/server/controller/tagrecorder"
	"github.com/deepflowio/deepflow/server/controller/trisolaris"

	_ "github.com/deepflowio/deepflow/server/controller/grpc/controller"
	_ "github.com/deepflowio/deepflow/server/controller/trisolaris/services/grpc/debug"
	_ "github.com/deepflowio/deepflow/server/controller/trisolaris/services/grpc/healthcheck"
	_ "github.com/deepflowio/deepflow/server/controller/trisolaris/services/grpc/synchronize"
	_ "github.com/deepflowio/deepflow/server/controller/trisolaris/services/http/cache"
	_ "github.com/deepflowio/deepflow/server/controller/trisolaris/services/http/upgrade"
)

var log = logging.MustGetLogger("controller")

type Controller struct{}

func Start(ctx context.Context, configPath, serverLogFile string, shared *servercommon.ControllerIngesterShared) {
	flag.Parse()

	serverCfg := config.DefaultConfig()
	serverCfg.Load(configPath)
	cfg := &serverCfg.ControllerConfig
	bytes, _ := yaml.Marshal(cfg)
	log.Info("==================== Launching DeepFlow-Server-Controller ====================")
	log.Infof("controller config:\n%s", string(bytes))
	setGlobalConfig(cfg)

	httpServer := http.NewServer(serverLogFile, cfg)
	httpServer.Start()

	defer router.SetInitStageForHealthChecker(router.OK)

	router.SetInitStageForHealthChecker("Election init")
	// start election
	go election.Start(ctx, cfg)

	isMasterController := IsMasterController(cfg)
	if isMasterController {
		router.SetInitStageForHealthChecker("MySQL migration")
		migrateMySQL(cfg)
	}

	router.SetInitStageForHealthChecker("MySQL init")
	// 初始化MySQL
	err := mysql.InitMySQL(cfg.MySqlCfg)
	if err != nil {
		log.Errorf("init mysql failed: %s", err.Error())
		time.Sleep(time.Second)
		os.Exit(0)
	}

	// 启动资源ID管理器
	router.SetInitStageForHealthChecker("Resource ID manager init")
	recorderdb.InitIDManager(&cfg.ManagerCfg.TaskCfg.RecorderCfg, ctx)
	if isMasterController {
		err := recorderdb.IDMNG.Start()
		if err != nil {
			log.Errorf("resource id mananger start failed: %s", err.Error())
			time.Sleep(time.Second)
			os.Exit(0)
		}
	}

	// 初始化Redis
	if cfg.RedisCfg.Enabled && cfg.TrisolarisCfg.NodeType == "master" {
		router.SetInitStageForHealthChecker("Redis init")

		err := redis.InitRedis(cfg.RedisCfg, ctx)
		if err != nil {
			log.Errorf("connect redis failed: %s", err.Error())
			time.Sleep(time.Second)
			os.Exit(0)
		}
	}

	router.SetInitStageForHealthChecker("Statsd init")
	// start statsd
	statsd.NewStatsdMonitor(cfg.StatsdCfg)

	router.SetInitStageForHealthChecker("Genesis init")
	// 启动genesis
	g := genesis.NewGenesis(cfg)
	g.Start()

	router.SetInitStageForHealthChecker("Manager init")
	// 启动resource manager
	// 每个云平台启动一个cloud和recorder
	m := manager.NewManager(cfg.ManagerCfg, shared.ResourceEventQueue)
	m.Start()

	router.SetInitStageForHealthChecker("Trisolaris init")
	// 启动trisolaris
	t := trisolaris.NewTrisolaris(&cfg.TrisolarisCfg, mysql.Db)
	go t.Start()

	prometheus := prometheus.GetSingleton()
	prometheus.SynchronizerCache.Start(ctx, &cfg.PrometheusCfg)
	prometheus.Encoder.Init(ctx, &cfg.PrometheusCfg)
	if isMasterController {
		prometheus.Encoder.Start()
	}

	router.SetInitStageForHealthChecker("TagRecorder init")
	tr := tagrecorder.NewTagRecorder(*cfg, ctx)
	go checkAndStartAllRegionMasterFunctions(tr)

	router.SetInitStageForHealthChecker("Master function init")
	controllerCheck := monitor.NewControllerCheck(cfg, ctx)
	analyzerCheck := monitor.NewAnalyzerCheck(cfg, ctx)
	go checkAndStartMasterFunctions(cfg, ctx, tr, controllerCheck, analyzerCheck)

	router.SetInitStageForHealthChecker("Register routers init")
	httpServer.SetAttrs(controllerCheck, analyzerCheck, m, g)
	httpServer.RegisterRouters()

	grpcStart(ctx, cfg)

	if !cfg.ReportingDisabled {
		go report.NewReportServer(mysql.Db).StartReporting()
	}
}

func grpcStart(ctx context.Context, cfg *config.ControllerConfig) {
	go grpc.Run(ctx, cfg)
}

func setGlobalConfig(cfg *config.ControllerConfig) {
	grpcPort, err := strconv.Atoi(cfg.GrpcPort)
	if err != nil {
		log.Error("config grpc-port is not a port")
		time.Sleep(time.Second)
		os.Exit(0)
	}
	grpcNodePort, err := strconv.Atoi(cfg.GrpcNodePort)
	if err != nil {
		log.Error("config grpc-node-port is not a port")
		time.Sleep(time.Second)
		os.Exit(0)
	}
	common.GConfig = &common.GlobalConfig{
		HTTPPort:     cfg.ListenPort,
		HTTPNodePort: cfg.ListenNodePort,
		GRPCPort:     grpcPort,
		GRPCNodePort: grpcNodePort,
	}
}
