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

package http

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/op/go-logging"

	"github.com/deepflowio/deepflow/server/controller/config"
	"github.com/deepflowio/deepflow/server/controller/genesis"
	"github.com/deepflowio/deepflow/server/controller/http/router"
	"github.com/deepflowio/deepflow/server/controller/http/router/configuration"
	"github.com/deepflowio/deepflow/server/controller/http/router/resource"
	"github.com/deepflowio/deepflow/server/controller/manager"
	"github.com/deepflowio/deepflow/server/controller/monitor"
	trouter "github.com/deepflowio/deepflow/server/controller/trisolaris/server/http"
	"github.com/deepflowio/deepflow/server/libs/logger"
)

var log = logging.MustGetLogger("http")

type Registrant interface {
	RegisterTo(*gin.Engine)
}

type Server struct {
	engine            *gin.Engine
	controllerConfig  *config.ControllerConfig
	controllerChecker *monitor.ControllerCheck
	analyzerChecker   *monitor.AnalyzerCheck
	manager           *manager.Manager
	genesis           *genesis.Genesis
}

func NewServer(logFile string, cfg *config.ControllerConfig) *Server {
	s := &Server{controllerConfig: cfg}

	ginLogFile, _ := os.OpenFile(logFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	gin.DefaultWriter = io.MultiWriter(ginLogFile, os.Stdout)

	g := gin.New()
	g.Use(gin.Recovery())
	g.Use(gin.LoggerWithFormatter(logger.GinLogFormat))
	s.engine = g
	return s
}

func (s *Server) SetAttrs(cc *monitor.ControllerCheck, ac *monitor.AnalyzerCheck, m *manager.Manager, g *genesis.Genesis) {
	s.controllerChecker = cc
	s.analyzerChecker = ac
	s.manager = m
	s.genesis = g
}

func (s *Server) Start() {
	router.NewHealth().RegisterTo(s.engine)
	go func() {
		if err := s.engine.Run(fmt.Sprintf(":%d", s.controllerConfig.ListenPort)); err != nil {
			log.Errorf("startup service failed, err:%v\n", err)
			time.Sleep(time.Second)
			os.Exit(0)
		}
	}()
}

func (s *Server) RegisterRouters() {
	for _, i := range []Registrant{
		router.NewElection(),
		router.NewDebug(s.manager, s.genesis),
		router.NewController(s.controllerConfig, s.controllerChecker),
		router.NewAnalyzer(s.controllerConfig, s.analyzerChecker),
		router.NewVtap(),
		router.NewVtapGroup(s.controllerConfig),
		router.NewDataSource(s.controllerConfig),
		router.NewVTapGroupConfig(),
		router.NewVTapInterface(),
		configuration.NewConfiguration(),
		router.NewVtapRepo(),
		router.NewPlugin(),
		resource.NewDomain(s.controllerConfig),
		resource.NewVPC(),
		resource.NewPod(s.controllerConfig.HTTPCfg, s.controllerConfig.RedisCfg, s.controllerConfig.FPermit),
		resource.NewProcess(s.controllerConfig.RedisCfg),
	} {
		i.RegisterTo(s.engine)
	}
	trouter.RegistRouter(s.engine)
}
