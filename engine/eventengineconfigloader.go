package engine

import (
	"strings"

	glog "github.com/golang/glog"
	inireader "github.com/go-ini/ini"
)

//Config - configuration file to load event engine related configurations
type Config struct {
	Name              string `ini:"name"`
	PartitionSize     uint64 `ini:"partitionsize"`
	PartitionLen      uint64 `ini:"partitionlength"`
	PartitionPosition uint64 `ini:"partitionposition"`
}

//EventEngineConfig - configuration file to load event engine related configurations
type EventEngineConfig struct {
	EventSource       string `ini:"source"`
	NatsServer        NatsServerConfig
	NatsFlowRoutes    []Route
	NatsRouteConfigs  map[string]NatsEngineRouteConfig
	EngineThrottel    ThrottelerConfig
	SLATicker         SLATickerConfig
	Profiler          ProfilerConfig
	PartitionSize     uint64
	PartitionPosition uint64
	PartitionLength   uint64
}

//Route - configuration to load parallel routes
type Route struct {
	Stagename string
	Nextstage *Route
	//Condition int    //Condition - check where there is any condition being set for this default to 0 = meaning no condition
	//CValue    string //CValue - value to be used for condition evaluation - this is defaulted to ""
}

//NatsServerConfig - configruation to load NATS Server related config
type NatsServerConfig struct {
	Type           string `ini:"type"`
	Port           int    `ini:"port"`
	HTTPPort       int    `ini:"httpport"`
	ClusterPort    int    `ini:"clusterport"`
	ProfPort       int    `ini:"profport"`
	ServerURL      string `ini:"serverurl"`
	MaxConnections int    `ini:"maxconnections"`
	EthName        string `ini:"ethname"`
	HostName       string `ini:"hostname"`
}

//NatsFlowRouteConfig - cofiguration to load NATS workflow
type NatsFlowRouteConfig struct {
	Route []string `ini:"route"`
}

//NatsEngineRouteConfig - configuration setting for each of the route paths
type NatsEngineRouteConfig struct {
	Name             string `ini:"name"`
	Subject          string `ini:"subject"`
	QueueGroup       string `ini:"queuegroup_publish"`
	SubjectACK       string `ini:"subject_ack"`
	Throttelcount    int64  `ini:"throttel_maxpending"`
	SLAThrottelCount int    `ini:"sla_throttel_count"`
	SLAInMillisec    int64  `ini:"sla_ticker_milliseconds"`
	ApplySeq         int    `ini:"insequence"`
	Wal              RocksDBConfig
}

//RocksDBConfig - configuration setting for RocksDB to be used
type RocksDBConfig struct {
	Name     string `ini:"name"`
	Datapath string `ini:"datafolderpath"`
}

//ThrottelerConfig - configuration setting for Throteller config to be used
type ThrottelerConfig struct {
	Maxpending int64 `ini:"throttel_maxpending"`
}

//SLATickerConfig - configuration setting for SLA-Ticker
type SLATickerConfig struct {
	SLAThrottelCount int   `ini:"sla_ticker_milliseconds"`
	SLAInMillisec    int64 `ini:"sla_throttel_count"`
}

//ProfilerConfig - configurations for profiler
type ProfilerConfig struct {
	Filename     string `ini:"filepath"`
	Scheduledfor int64  `ini:"scheduled_in_minutes"`
}

//ParseConfig - function to parse entire config path and load corresponding values
func (engine *EventEngine) ParseConfig() {
	//load config file from configuration reader
	cfg, cfgerr := inireader.Load(*engine.Configpath)
	if cfgerr != nil {
		glog.Errorf("Error loading data from configuration file provided. Error: %s\n", cfgerr)
		panic("Error loading data from configuration file provided")
	}

	//Load engine section
	config := new(Config)
	cfgerr = cfg.Section("engine").MapTo(config)
	if cfgerr != nil {
		glog.Errorf("Error loading data from [events]. Error: %s\n", cfgerr)
		panic("Error loading data from [events]")
	}
	glog.Infof("Config load, partition length set: %d \n", config.PartitionLen)

	//Load default configurations if any
	engineConfig := new(EventEngineConfig)
	cfgerr = cfg.MapTo(engineConfig)
	if cfgerr != nil {
		glog.Errorf("Error loading data from Engine related configuration file. Error: %s\n", cfgerr)
		panic("Error loading data from Engine related configuration file")
	}

	//Load events section
	cfgerr = cfg.Section("events").MapTo(engineConfig)
	if cfgerr != nil {
		glog.Errorf("Error loading data from [events]. Error: %s\n", cfgerr)
		panic("Error loading data from [events]")
	}
	//Load nats section
	natsConfig := new(NatsServerConfig)
	cfgerr = cfg.Section("nats").MapTo(natsConfig)
	if cfgerr != nil {
		glog.Errorf("Error loading data from [nats]. Error: %s\n", cfgerr)
		panic("Error loading data from [nats]")
	}
	engineConfig.NatsServer = *natsConfig
	engineConfig.PartitionSize = config.PartitionSize
	engineConfig.PartitionPosition = config.PartitionPosition
	engineConfig.PartitionLength = config.PartitionLen
	//glog.Infof("Partitionsize set: %d", config.PartitionSize)
	//Load nats.flow section
	natsRouteConfig := new(NatsFlowRouteConfig)
	cfgerr = cfg.Section("nats.flow").MapTo(natsRouteConfig)
	if cfgerr != nil {
		glog.Errorf("Error loading data from [nats.flow]. Error: %s\n", cfgerr)
		panic("Error loading data from [nats.flow]")
	}
	if len(natsRouteConfig.Route) > 0 {
		routeConfigMap := make(map[string]NatsEngineRouteConfig)
		routes := make([]Route, len(natsRouteConfig.Route))
		for i := range natsRouteConfig.Route {
			currentRoute := natsRouteConfig.Route[i]
			flowRoute := strings.Split(currentRoute, "->")
			//also load the corresponding routeconfigs for each route
			var newRoute *Route
			var prevRoute *Route
			for j := range flowRoute {
				natsEngineRouteConfig := new(NatsEngineRouteConfig)
				currentsectionname := []byte("nats.flow.")
				currentroutebytearray := []byte(strings.TrimSpace(flowRoute[j]))
				for k := range currentroutebytearray {
					currentsectionname = append(currentsectionname, currentroutebytearray[k])
				}
				cfgerr = cfg.Section(string(currentsectionname)).MapTo(natsEngineRouteConfig)
				if cfgerr != nil {
					glog.Errorf("Failed to load flow related configurations. Error: %s\n", cfgerr)
					panic("Failed to load flow related configurations")
				}
				//set Route settings
				route := new(Route)
				route.Stagename = string(currentsectionname)
				route.Nextstage = nil
				//route.Condition = 0
				//route.CValue = ""
				if prevRoute != nil {
					prevRoute.Nextstage = route
				}
				if newRoute == nil {
					newRoute = route
				}
				prevRoute = route
				//get wal related settings for this route
				natsRouteWalConfig := new(RocksDBConfig)
				currentsectionname = []byte("nats.flow.rocksdb.wal.")
				for k := range currentroutebytearray {
					currentsectionname = append(currentsectionname, currentroutebytearray[k])
				}
				cfgerr = cfg.Section(string(currentsectionname)).MapTo(natsRouteWalConfig)
				if cfgerr != nil {
					glog.Errorf("Failed to load wal related configurations. Error: %s\n", cfgerr)
					panic("Failed to load wal related configurations")
				}
				if len(natsEngineRouteConfig.Name) > 0 {
					natsEngineRouteConfig.Wal = *natsRouteWalConfig
					routeConfigMap[natsEngineRouteConfig.Name] = *natsEngineRouteConfig
				} else {
					glog.Infof("Config related information not found for: %s", currentsectionname)
				}
			}
			routes[i] = *newRoute
		}
		engineConfig.NatsFlowRoutes = routes
		engineConfig.NatsRouteConfigs = routeConfigMap
	} else {
		glog.Errorln("Could not find data for [nats.flow] related configuration. Cannot process any data.")
		panic("Could not find data for [nats.flow] related configuration")
	}
	//Load engine Throttel section
	throttelConfig := new(ThrottelerConfig)
	cfgerr = cfg.Section("throtteling").MapTo(throttelConfig)
	if cfgerr != nil {
		glog.Errorf("Error loading data from [throtteling]. Error: %s\n", cfgerr)
		panic("Error loading data from [throtteling]")
	}
	engineConfig.EngineThrottel = *throttelConfig

	//Load engine SLA section
	slaConfig := new(SLATickerConfig)
	cfgerr = cfg.Section("sla_ticker").MapTo(slaConfig)
	if cfgerr != nil {
		glog.Errorf("Error loading data from [sla_ticker]. Error: %s\n", cfgerr)
		panic("Error loading data from [sla_ticker]")
	}
	engineConfig.SLATicker = *slaConfig
	//Load engine profiler section
	profileConfig := new(ProfilerConfig)
	cfgerr = cfg.Section("profiler").MapTo(profileConfig)
	if cfgerr != nil {
		glog.Errorf("Error loading data from [sla_ticker]. Error: %s\n", cfgerr)
		panic("Error loading data from [sla_ticker]")
	}
	engineConfig.Profiler = *profileConfig

	engine.EventEngineConfig = *engineConfig
}
