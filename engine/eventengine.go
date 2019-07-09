package engine

import (
	"bytes"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	http "net/http"

	glog "github.com/glog-master"
	inireader "github.com/go-ini/ini"
)

//EventEngine - Engine related configuration to be setup and captured in GOSS
type EventEngine struct {
	EventEngineConfig
	Name           string
	Configpath     *string
	cores          []coreEngine
	throttler      *Throttler
	condMutex      *sync.Mutex
	cond           *sync.Cond
	overloaded     int32
	keeppublishing bool
	natsserver     *NatsCoreServer
	isEmbeddedNats bool
}

//coreEngine - core engine resource references to be managed and used by Event Engine
type coreEngine struct {
	name           string
	route          Route
	processingUnit map[string]*coreProcessingUnit
	processerror   error
}

//coreProcessingUnit - core engine resource references to be managed and used by Event Engine
type coreProcessingUnit struct {
	publisher         *NatsPublisher
	subscriber        *NatsSubscriber
	walstore          *RocksDBInstance
	throttler         *Throttler
	stagename         string
	thsafe            *sync.Mutex
	thsafeD           sync.Mutex
	parseEventRef     func(kv RocksData, partitionkey uint64, partitionsize uint64, partitionlength uint64) (string, int64, string)
	core              *coreEngine
	profiler          *Profiler
	insequence        bool
	subscriberset     chan bool
	partitionsize     uint64
	partitionposition uint64
	partitionlength   uint64
}

/*
RocksData json event type declaration as well as interface
*/
type RocksData struct {
	Event string
}

/*
ParseEvent declaration as interface
*/
type ParseEvent interface {
	GetKeysAndSnapshot() (string, int64)
}

/*
SetEventParserForKeySnapshot - function used to hold functional pointer
*/
func (processingUnit *coreProcessingUnit) SetEventParserForKeySnapshot(parseCB func(kv RocksData, partitionkey uint64, partitionsize uint64, partitionlength uint64) (string, int64, string)) {
	processingUnit.parseEventRef = parseCB
}

/*
CreateEventsEngine - function to initialize event engine
*/
func CreateEventsEngine(configpath *string) EventEngine {
	engine := new(EventEngine)
	cfg, cfgerr := inireader.Load(*configpath)
	if cfgerr != nil {
		glog.Errorf("Error loading data from configuration file provided. Error: %s\n", cfgerr)
		panic("Error loading data from configuration file provided")
	}

	enginesection, cfgerr := cfg.GetSection("engine")
	if cfgerr != nil {
		glog.Errorf("Error reading engine name for config file. Error: %s\n", cfgerr)
		panic("Error reading engine name for config file")
	}

	engine.Name = enginesection.Key("name").String()
	engine.Configpath = configpath
	engine.keeppublishing = true
	engine.condMutex = &sync.Mutex{}
	engine.cond = sync.NewCond(engine.condMutex)
	engine.ParseConfig()
	//If required startlocalNATS
	engine.startNatsServer()
	//Set engine Throttel
	throttel := new(Throttler)
	throttel.Maxpending = engine.EngineThrottel.Maxpending
	throttel.SLAInMillisec = engine.SLATicker.SLAInMillisec
	throttel.SLAcount = engine.SLATicker.SLAThrottelCount
	engine.throttler = throttel
	engine.throttler.CreateEngineThrotteler()
	//procced with initializing core and other operational settings
	var coresCountRequired int
	if len(engine.EventEngineConfig.NatsRouteConfigs) > 0 {
		coresCountRequired = len(engine.EventEngineConfig.NatsRouteConfigs) * 7
	} else {
		coresCountRequired = 7
	}

	//Try to reset the core count based on the availability of CPU cores
	glog.Infof("Current CPU cores available on the machine to process: %d\n", runtime.NumCPU())
	if runtime.NumCPU() < coresCountRequired {
		runtime.GOMAXPROCS(runtime.NumCPU())
		glog.Infof("Current CPU cores set for the event engine to process: %d\n", runtime.NumCPU())
	} else {
		runtime.GOMAXPROCS(coresCountRequired)
		glog.Infof("Current CPU cores set for the event engine to process: %d\n", coresCountRequired)
	}
	engine.initializeCoreEngine()
	return *engine
}

//startNatsServer - function to start NATS server
func (engine *EventEngine) startNatsServer() {
	engine.isEmbeddedNats = false
	if len(engine.NatsServer.Type) > 0 {
		if strings.Compare(engine.NatsServer.Type, "embedded") == 0 {
			engine.isEmbeddedNats = true
			engine.natsserver = new(NatsCoreServer)
			engine.natsserver.Options = engine.natsserver.Getoptions(engine.NatsServer.Port,
				engine.NatsServer.HTTPPort,
				engine.NatsServer.ClusterPort,
				engine.NatsServer.ProfPort,
				engine.NatsServer.MaxConnections,
				engine.NatsServer.HostName,
				engine.NatsServer.EthName)
			if engine.natsserver.StartNatsCore() {
				engine.NatsServer.ServerURL = engine.natsserver.GetConnectionURL()
				glog.Infoln("Embedded nats server started and running successfully")
				glog.Infof("Server started with connection string %s", engine.NatsServer.ServerURL)
			} else {
				panic("Failed to start embedded server. Please check with ASTRA admin")
			}
		}
	}
}

//clienthold - function to go on client hold
func (engine *EventEngine) clienthold() {
	if engine.natsserver.GetNumberOfClient() == 0 {
		glog.Infoln("No Clients are connected and hence going on hold...")
	}
}

//shutdownNatsServer - function to shutdown NATS server
func (engine *EventEngine) shutdownNatsServer() {
	if engine.isEmbeddedNats {
		engine.natsserver.StopNatsCore()
	}
}

//initializeCoreEngine - function to initialize core engine
func (engine *EventEngine) initializeCoreEngine() {
	//Generate core engine memory instances from Engine
	coreEngineUnits := make([]coreEngine, len(engine.NatsFlowRoutes))
	for count := range engine.NatsFlowRoutes {
		processingUnitMap := make(map[string]*coreProcessingUnit)
		coreEngineUnit := new(coreEngine)
		coreEngineUnit.route = engine.NatsFlowRoutes[count]
		coreEngineUnit.name = engine.NatsFlowRoutes[count].Stagename
		natsEngineRouteConfig := engine.NatsRouteConfigs[engine.NatsFlowRoutes[count].Stagename]
		processingUnitMap[natsEngineRouteConfig.Name] = initializeProcessingUnit(&natsEngineRouteConfig,
			engine.NatsServer.ServerURL,
			coreEngineUnit,
			&engine.EventEngineConfig,
			engine.EventEngineConfig.PartitionSize,
			engine.EventEngineConfig.PartitionPosition,
			engine.EventEngineConfig.PartitionLength)
		glog.Infof("parition length set: %d\n", engine.EventEngineConfig.PartitionLength)
		if len(engine.NatsFlowRoutes) > 0 {
			currentRoute := engine.NatsFlowRoutes[count].Nextstage
			for currentRoute != nil {
				natsEngineRouteConfig = engine.NatsRouteConfigs[currentRoute.Stagename]
				processingUnitMap[natsEngineRouteConfig.Name] = initializeProcessingUnit(&natsEngineRouteConfig,
					engine.NatsServer.ServerURL,
					coreEngineUnit,
					&engine.EventEngineConfig,
					engine.EventEngineConfig.PartitionSize,
					engine.EventEngineConfig.PartitionPosition,
					engine.EventEngineConfig.PartitionLength)
				currentRoute = currentRoute.Nextstage
			}
		}
		coreEngineUnit.processingUnit = processingUnitMap
		coreEngineUnits[count] = *coreEngineUnit
		glog.Infof("Core engine created successfully for flow: %s\n", coreEngineUnit.route.Stagename)
	}
	engine.cores = coreEngineUnits
	glog.Infof("Event Engine cores created successfully")
}

//initializeProcessingUnit - function to initialize core processing unit
func initializeProcessingUnit(natsEngineRouteConfig *NatsEngineRouteConfig,
	natsServerurl string,
	engine *coreEngine,
	engineconfig *EventEngineConfig,
	PartitionSize uint64,
	PartitionPosition uint64,
	PartitionLength uint64) *coreProcessingUnit {
	coreEngineProcessingUnit := new(coreProcessingUnit)
	coreEngineProcessingUnit.stagename = natsEngineRouteConfig.Name
	coreEngineProcessingUnit.publisher = initializePublisher(natsEngineRouteConfig, natsServerurl)
	coreEngineProcessingUnit.walstore = initializeWal(natsEngineRouteConfig)
	coreEngineProcessingUnit.throttler = initializeThrotteler(natsEngineRouteConfig)
	coreEngineProcessingUnit.throttler.PartitionSize = PartitionSize
	coreEngineProcessingUnit.throttler.PartitionPosition = PartitionPosition
	coreEngineProcessingUnit.throttler.PartitionLength = PartitionLength
	coreEngineProcessingUnit.subscriber = initializeSubscriber(natsEngineRouteConfig, natsServerurl)
	coreEngineProcessingUnit.profiler = initializeProfiler(engineconfig, natsEngineRouteConfig.Name)
	coreEngineProcessingUnit.thsafe = &sync.Mutex{}
	coreEngineProcessingUnit.thsafeD = sync.Mutex{}
	coreEngineProcessingUnit.core = engine
	coreEngineProcessingUnit.insequence = false
	glog.Infof("Stage name [%s] with sequence as [%d]", coreEngineProcessingUnit.stagename, natsEngineRouteConfig.ApplySeq)
	if natsEngineRouteConfig.ApplySeq == 1 {
		coreEngineProcessingUnit.insequence = true
	}
	coreEngineProcessingUnit.subscriberset = make(chan bool, 1)
	coreEngineProcessingUnit.partitionsize = PartitionSize
	coreEngineProcessingUnit.partitionposition = PartitionPosition
	coreEngineProcessingUnit.partitionlength = PartitionLength
	return coreEngineProcessingUnit
}

//initializePublisher - function to initialize publisher for core processing unit
func initializePublisher(natsEngineRouteConfig *NatsEngineRouteConfig, serverurl string) *NatsPublisher {
	//Create publisher for core processing unit
	if len(natsEngineRouteConfig.Subject) > 0 {
		publisher := new(NatsPublisher)
		publisher.Defaulturls = serverurl
		publisher.Name = natsEngineRouteConfig.Name
		publisher.Subject = natsEngineRouteConfig.Subject
		publisher.Queuegroup = natsEngineRouteConfig.QueueGroup
		err := publisher.ConnectNats()
		if err != nil {
			glog.Errorf("Failed while creating publisher with name: %s\n", publisher.Name)
			panic("Failed while creating event engine")
		}
		glog.Infof("Publisher %s created successfully\n", publisher.Name)
		return publisher
	}
	return nil
}

//initializeSubscriber - function to initialize subscriber for core processing unit
func initializeSubscriber(natsEngineRouteConfig *NatsEngineRouteConfig, serverurl string) *NatsSubscriber {
	//Create subscriber for core processing unit
	if len(natsEngineRouteConfig.SubjectACK) > 0 {
		subscriber := new(NatsSubscriber)
		subscriber.Defaulturls = serverurl
		subscriber.Name = natsEngineRouteConfig.Name
		subscriber.Subject = natsEngineRouteConfig.SubjectACK
		err := subscriber.ConnectNatsAckSubscriber()
		if err != nil {
			glog.Errorf("Failed while creating subscriber with name: %s\n", subscriber.Name)
			panic("Failed while creating event engine")
		}
		glog.Infof("Subscriber %s created successfully\n", subscriber.Name)
		return subscriber
	}
	return nil
}

//initializeWal - function to initialize wal for core processing unit
func initializeWal(natsEngineRouteConfig *NatsEngineRouteConfig) *RocksDBInstance {
	//Create WAL for publisher
	if len(natsEngineRouteConfig.Name) > 0 {
		wal := new(RocksDBInstance)
		wal.Name = natsEngineRouteConfig.Wal.Name
		wal.Datapath = natsEngineRouteConfig.Wal.Datapath
		err := wal.ConnectRocksDB()
		if err != nil {
			glog.Errorf("Failed while creating Wal with name: %s\n", wal.Name)
			panic("Failed while creating event engine")
		}
		glog.Infof("Wal %s created successfully\n", wal.Name)
		return wal
	}
	return nil
}

//initializeThrotteler - function to initialize throtteler for core processing unit
func initializeThrotteler(natsEngineRouteConfig *NatsEngineRouteConfig) *Throttler {
	//Create Throttler for publisher - if publisher available only then start the throtteler
	if len(natsEngineRouteConfig.Subject) > 0 {
		throtteler := new(Throttler)
		throtteler.Name = natsEngineRouteConfig.Name
		throtteler.Maxpending = natsEngineRouteConfig.Throttelcount
		throtteler.SLAInMillisec = natsEngineRouteConfig.SLAInMillisec
		throtteler.SLAcount = natsEngineRouteConfig.SLAThrottelCount
		throtteler.CreateEngineThrotteler()
		glog.Infof("Throtteler %s created successfully\n", natsEngineRouteConfig.Name)
		return throtteler
	}
	return nil
}

//initializeProfiler - function to initialize profiler for core processing unit
func initializeProfiler(engineconfig *EventEngineConfig, name string) *Profiler {
	if len(engineconfig.Profiler.Filename) > 0 {
		return CreateProfiler(engineconfig.Profiler.Filename, engineconfig.Profiler.Scheduledfor, name)
	}
	return nil
}

//initializeHTTP - function to initialize http handler service
func (engine *EventEngine) initializeHTTP() {
	http.Handle("/pending/count", http.HandlerFunc(engine.GetPendingCount))
}

//GetPendingCount - function to get pending count
func (engine *EventEngine) GetPendingCount(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	buf := new(bytes.Buffer)
	defer func() {
		buf.Reset()
		buf = nil
	}()
	for core := range engine.cores {
		for punit := range engine.cores[core].processingUnit {
			name := engine.cores[core].processingUnit[punit].stagename
			count := engine.cores[core].processingUnit[punit].throttler.GetThrottelerEventCount()
			buf.WriteString(name)
			buf.WriteString(":")
			buf.WriteString(strconv.FormatInt(int64(count), 10))
			buf.WriteString("\n")
		}
	}
	fmt.Fprintf(w, buf.String())
}

//Shutdown - function to shutdown event engine
func (engine *EventEngine) Shutdown() {
	//shutdown each core
	glog.Infof("Starting to shutdown engine %s", engine.Name)
	engine.keeppublishing = false
	glog.Infoln("Release any holds for the event engine if applicable")
	engine.ReleaseLoad()
	glog.Infof("Publishing of data stopped")
	for core := range engine.cores {
		engine.cores[core].shutdownCore()
		glog.Infof("Shutdown of core engine %s successful", engine.cores[core].name)
	}
	engine.shutdownNatsServer()
	glog.Infof("Shutdown of engine %s successful", engine.Name)
}

/*
StopPublish - funtion to stop publishing
*/
func (engine *EventEngine) StopPublish() {
	//shutdown each core
	glog.Infof("Stopping publish from engine: %s", engine.Name)
	engine.keeppublishing = false
	glog.Infof("Publish stopped for engine %s successful", engine.Name)
}

/*
ResetPublish - funtion to stop publishing
*/
func (engine *EventEngine) ResetPublish() {
	//shutdown each core
	glog.Infof("Reset publish for engine: %s", engine.Name)
	engine.keeppublishing = true
	glog.Infof("Reset stopped for engine %s successful", engine.Name)
}

/*
ShutdownWithNotification - notification on process going down
*/
func (engine *EventEngine) ShutdownWithNotification(notifykill func()) {
	notifykill()
	//shutdown each core
	glog.Infof("Starting to shutdown engine %s", engine.Name)
	engine.keeppublishing = false
	glog.Infoln("Release any holds for the event engine if applicable")
	engine.ReleaseLoad()
	glog.Infof("Publishing of data stopped")
	for core := range engine.cores {
		engine.cores[core].shutdownCore()
		glog.Infof("Shutdown of core engine %s successful", engine.cores[core].name)
	}
	glog.Infof("Shutdown of engine %s successful", engine.Name)
}

func (core *coreEngine) getCore() **coreEngine {
	return &core
}

//shutdownCore - function to shutdown each core
func (core *coreEngine) shutdownCore() {
	for procUnit := range core.processingUnit {
		if len(core.processingUnit[procUnit].stagename) > 0 {
			glog.Infof("Starting to shutdown of processing unit %s", core.processingUnit[procUnit].stagename)
			processingUnit := core.processingUnit[procUnit]
			processingUnit.shutdownProcessingUnit()
			glog.Infof("Shutdown of processing unit %s successful", core.processingUnit[procUnit].stagename)
		}
	}
}

//shutdownProcessingUnit - function to shutdown processing unit
func (processingUnit *coreProcessingUnit) shutdownProcessingUnit() {
	//Close profiler
	if len(processingUnit.profiler.Filename) > 2 {
		glog.Infoln("Stopping profilers..")
		processingUnit.profiler.StopCPUProfiling()
		processingUnit.profiler.StopMemoryProfiling()
	}
	//Close publisher
	glog.Infof("Stopping Publishers..")
	shutdownNATSPublisher(processingUnit.publisher)
	//close sla ticker
	glog.Infof("Starting to shutdown of throtteler for publisher: %s", processingUnit.publisher.Name)
	shutdownThrotteler(processingUnit.throttler)
	//close subscriber
	glog.Infof("Stopping Subscribers..")
	shutdownNATSSubscriber(processingUnit.subscriber)
	//close wal
	glog.Infof("Closing Wal..")
	shutdownWalStore(processingUnit.walstore)

}

//shutdownNATSPublisher - function to shutdown NATS Publisher
func shutdownNATSPublisher(publisher *NatsPublisher) {
	glog.Infof("Starting to shutdown of publisher: %s", publisher.Name)
	if publisher != nil {
		publisher.Natserr = publisher.CloseNats()
		if publisher.Natserr != nil {
			glog.Errorf("Shutting down of publisher %s failed with Error: %s\n", publisher.Name, publisher.Natserr)
		} else {
			glog.Infof("Shutdown of publisher: %s successful", publisher.Name)
		}
	}
}

//shutdownNATSSubscriber - function to shutdown NATS Publisher
func shutdownNATSSubscriber(subscriber *NatsSubscriber) {
	glog.Infof("Starting to shutdown of subscriber: %s", subscriber.Name)
	if subscriber != nil {
		subscriber.Natserr = subscriber.CloseNatsSubscriber()
		if subscriber.Natserr != nil {
			glog.Errorf("Shutting down of subscriber %s failed with Error: %s\n", subscriber.Name, subscriber.Natserr)
		} else {
			glog.Infof("Shutdown of subscriber: %s successful", subscriber.Name)
		}
	}
}

//shutdownWalStore - function to shutdown WAL store
func shutdownWalStore(wal *RocksDBInstance) {
	glog.Infof("Starting to shutdown of WAL: %s", wal.Name)
	if wal != nil {
		wal.Err = wal.CloseRocksDB()
		if wal.Err != nil {
			glog.Errorf("Shutting down of wal %s failed with Error: %s\n", wal.Name, wal.Err)
		} else {
			glog.Infof("Shutdown of WAL: %s successful", wal.Name)
		}
	}
}

//shutdownThrotteler - function to shutdown throtteler
func shutdownThrotteler(throtteler *Throttler) {
	if throtteler != nil {
		throtteler.StopSLATicker()
	}
	glog.Infoln("Shutdown of throtteler for publisher successful")
}

/*
function to set nats ack subscription
Event expected back in the form of RocksDBKV
*/
func (processingUnit *coreProcessingUnit) startACKSubscription(engine *EventEngine) {
	//wgsub.Add(1)
	go processingUnit.subscriber.SubscribeACKs(func(data string) {
		starttime := time.Now().UnixNano() / int64(time.Millisecond)
		glog.Infof("Got an ACK for key: %s\n", data)
		processingUnit.thsafeD.Lock()
		err := processingUnit.processAckevent(data, engine)
		if err != nil {
			glog.Errorf("Error processing events from recevied as ack. Error detials: %s\n", err)
			//no panic here?? should I panic -- JP
		}
		processingUnit.thsafeD.Unlock()
		glog.Infof("Time taken to process ACK for engine [%s] is %d", processingUnit.stagename, (time.Now().UnixNano()/int64(time.Millisecond))-starttime)
	})
	glog.Infoln("Started ACK subscription process...")
	processingUnit.subscriberset <- true
	//wgsub.Wait()
}

/*
ProcessAckevent - function to process event that got processed and received confirmation back
Event expected back in the form of RocksDBKV
*/
func (processingUnit *coreProcessingUnit) processAckevent(kv string, engine *EventEngine) error {
	if len(kv) > 0 {
		//Process ACK Event here
		rocksData := new(RocksData)
		rocksData.Event = kv
		key, _, seq := processingUnit.parseEventRef(*rocksData, processingUnit.partitionsize, processingUnit.partitionposition, processingUnit.partitionlength)

		err := processingUnit.core.processNextStage(key, kv, seq, processingUnit.stagename)
		if err != nil {
			glog.Errorf("Failed to publish events to next stage. Current stage [%s]. Err: %s\n", processingUnit.stagename, err)
			return err
		}
		//flag, err := processingUnit.walstore.DeletefromRocksDB(key)
		_, err = processingUnit.walstore.DeletefromRocksDB(key)
		if err != nil {
			glog.Errorf("Failed to delete events from RocksDB. Err: %s\n", err)
			return err
		}

		//This is causing the throttler issues on SLA ticker hence disabling this logic till I clean throtteler - JP
		if processingUnit.throttler.UpdateACKAndThrottel() {
			engine.ReleaseLoad()
		}

		/* This is commented for disabling using flag and replaced with above
		if flag {
			if processingUnit.throttler.UpdateACKAndThrottel() {
				engine.ReleaseLoad()
			}
		}
		*/
	}
	return nil
}

/*
processNextStage - function to process next stage of workflow once ACK is received from Subscriber
Currently load throtteling is not implemented in corresponding stages of workflow
Still have to make up my mind to throtteling workflow subsequent process makesense???? --- :(  JP
*/
func (core *coreEngine) processNextStage(key string, event string, seq string, currentstage string) error {
	var currentRoute *Route
	route := &core.route
	notFound := true
	for route != nil && notFound {
		if strings.Compare(core.route.Stagename, currentstage) == 0 {
			currentRoute = &core.route
			notFound = false
		}
		route = route.Nextstage
	}
	if currentRoute != nil {
		if currentRoute.Nextstage != nil {
			//need to check if key exists in rocks db - to avoid duplication. If not in rocks assume that as duplicate
			val, err := core.processingUnit[currentRoute.Stagename].walstore.ReadfromRocksDB(key)
			if err != nil {
				glog.Errorf("Failed to read ack key from wal. Current stage [%s]. Err: %s\n", currentRoute.Stagename, err)
				return err
			}
			if len(val) > 0 {
				//process event for this stage
				processingUnit := core.processingUnit[currentRoute.Nextstage.Stagename]
				//var err error
				glog.Infof("Processing unit [%s] with seq set to %b", processingUnit.stagename, processingUnit.insequence)
				if processingUnit.insequence && (seq != "") {
					_, err = processingUnit.PublisheventInSEQ(key, event, seq)
				} else {
					_, err = processingUnit.Publishevent(key, event)
				}
				if err != nil {
					return err
				}
			} else {
				glog.Errorf("Publish of message not processed as it is concieved as duplicate message. Current stage [%s]. key: %s\n", currentRoute.Nextstage.Stagename, key)
			}
		}
	}
	return nil
}

/*
Function to resend all pending events from event engine that are currently on hold
*/
func (processingUnit *coreProcessingUnit) releasePendingEvents(engine *EventEngine) error {
	//releasepending := true
	//limit := 10
	//start := 1
	//for releasepending {
	glog.Infof("Releasing pending event to NATS [%s].\n", processingUnit.stagename)
	//readrange := EventrangeRocksDB{start, limit}
	events, err := processingUnit.walstore.ReadRocksDBEntriesAll()
	if err != nil {
		return err
	}
	glog.Infof("Curent event to process in NATS [%s]: %d\n", processingUnit.stagename, len(events))
	for key := range events {
		//if len(events[i]) > 0 {
		kv := RocksData{events[key]}
		ke, _, seq := processingUnit.parseEventRef(kv, processingUnit.partitionsize, processingUnit.partitionposition, processingUnit.partitionlength)
		glog.Infof("Releasing pending event to NATS [%s]. Event key: %s of length:%d with sequence_id[%s]\n", processingUnit.stagename, ke, len(kv.Event), seq)
		//if (len(ke) <= 0) || (strings.Compare(ke, " ") == 0) { //Not sure how this got in...serious bug introduced by GCM NON US
		if len(ke) <= 0 {
			processingUnit.walstore.DeletefromRocksDB(ke)
		} else {
			if processingUnit.insequence && seq != "" {
				err = processingUnit.publisher.PublishtoqueuegroupInSeq(kv.Event, seq)
			} else {
				err = processingUnit.publisher.Publishtoqueuegroup(kv.Event)
			}
			if err != nil {
				//need to log err and handle connection reset here
				return err
			}
		}
		glog.Infof("Messages published to NATS [%s]. Event key: %s\n", processingUnit.stagename, key)
		if processingUnit.throttler.UpdateEventPublishedAndThrottel(1) {
			engine.NotifyOnLoad()
		}
	}
	//start += limit
	//}
	return nil
}

/*
GetEventsFromLocalPersistance - function to get events from local persistance
*/
func (processingUnit *coreProcessingUnit) GetEventsFromLocalPersistance(count int) (map[int64]string, error) {
	keys := make(map[int64]string)
	readrange := EventrangeRocksDB{1, count}
	events, err := processingUnit.walstore.ReadRocksDBonRange(readrange)
	if err != nil {
		return keys, err
	}
	for i := 0; i < int(count); i++ {
		if len(events[i]) > 0 {
			var evt RocksData
			evt = RocksData{events[i]}
			//parse key value from Rocks DB and return key along with current snapshot time
			_, snapshot, _ := processingUnit.parseEventRef(evt, processingUnit.partitionsize, processingUnit.partitionposition, processingUnit.partitionlength)
			if snapshot == 0 {
				snapshot = int64(i) //Temp fix added by JP to get away from string based manipulation of snapshot that is not derived from messages.
			}
			keys[snapshot] = events[i]
		}
	}
	return keys, err
}

/*
PublisheventNoThrottel - function to publish event to event engine
*/
func (processingUnit *coreProcessingUnit) PublisheventNoThrottel(event string, seq string) error {
	//publish to NATS
	var err error
	if (processingUnit.insequence) && (seq != "") {
		err = processingUnit.publisher.PublishtoqueuegroupInSeq(event, seq)
	} else {
		err = processingUnit.publisher.Publishtoqueuegroup(event)
	}
	if err != nil {
		//need to log err and handle connection reset here
		//resetNATS()
		return err
	}
	return nil
}

/*
Publishevent - function to publish event to event engine
*/
func (processingUnit *coreProcessingUnit) Publishevents(events map[string]string) error {
	for m := range events {
		//generate key value to be stored in rocksdb and corresponding JSON data
		//key := strconv.FormatInt((time.Now().UnixNano() / int64(time.Millisecond)), 10)
		//store into Rocks DB
		err := processingUnit.walstore.WritetoRocksDB(m, events[m])
		if err != nil {
			//need to log err and handle connection reset here
			//resetrocksdb()
			return err
		}
		//publish to NATS
		err = processingUnit.publisher.Publishtoqueuegroup(events[m])
		if err != nil {
			//need to log err and handle connection reset here
			//resetNATS()
			return err
		}
		processingUnit.throttler.UpdateEventPublishedAndThrottel(1)
	}
	return nil
}

/*
Publishevent - function to publish event to event engine
*/
func (processingUnit *coreProcessingUnit) Publishevent(key string, event string) (bool, error) {
	//generate key value to be stored in rocksdb and corresponding JSON data
	//key := strconv.FormatInt((time.Now().UnixNano() / int64(time.Millisecond)), 10)
	//store into Rocks DB
	err := processingUnit.walstore.WritetoRocksDB(key, event)
	if err != nil {
		//need to log err and handle connection reset here
		//resetrocksdb()
		return false, err
	}
	//publish to NATS
	err = processingUnit.publisher.Publishtoqueuegroup(event)
	if err != nil {
		//need to log err and handle connection reset here
		//resetNATS()
		return false, err
	}
	return processingUnit.throttler.UpdateEventPublishedAndThrottel(1), nil
}

/*
PublisheventBatch - function to publish event to event engine
*/
func (processingUnit *coreProcessingUnit) PublisheventBatch(kv map[string]string) (bool, error) {
	//generate key value to be stored in rocksdb and corresponding JSON data
	//key := strconv.FormatInt((time.Now().UnixNano() / int64(time.Millisecond)), 10)
	//store into Rocks DB
	err := processingUnit.walstore.WriteBatchtoRocksDB(kv)
	if err != nil {
		//need to log err and handle connection reset here
		//resetrocksdb()
		return false, err
	}

	for k := range kv {
		//publish to NATS
		err = processingUnit.publisher.Publishtoqueuegroup(kv[k])
		if err != nil {
			//need to log err and handle connection reset here
			//resetNATS()
			return false, err
		}
	}

	return processingUnit.throttler.UpdateEventPublishedAndThrottel(int64(len(kv))), nil
}

/*
PublisheventInSEQ - function to publish event to event engine
*/
func (processingUnit *coreProcessingUnit) PublisheventInSEQ(key string, event string, seq string) (bool, error) {
	//generate key value to be stored in rocksdb and corresponding JSON data
	//key := strconv.FormatInt((time.Now().UnixNano() / int64(time.Millisecond)), 10)
	//store into Rocks DB
	starttime := time.Now().UnixNano() / int64(time.Millisecond)
	err := processingUnit.walstore.WritetoRocksDB(key, event)
	glog.Infof("Time taken to save one OmegaEvent in WAL: %d", ((time.Now().UnixNano() / int64(time.Millisecond)) - starttime))
	if err != nil {
		//need to log err and handle connection reset here
		//resetrocksdb()
		return false, err
	}
	//publish to NATS
	starttime = time.Now().UnixNano() / int64(time.Millisecond)
	err = processingUnit.publisher.PublishtoqueuegroupInSeq(event, seq)
	glog.Infof("Time taken to publish one OmegaEvent in QG[%s]: %d", processingUnit.publisher.Queuegroup, ((time.Now().UnixNano() / int64(time.Millisecond)) - starttime))
	if err != nil {
		//need to log err and handle connection reset here
		//resetNATS()
		return false, err
	}
	//starttime = time.Now().UnixNano() / int64(time.Millisecond)
	update := processingUnit.throttler.UpdateEventPublishedAndThrottel(1)
	//glog.Infof("Time taken to update the throteller for one event: %d", ((time.Now().UnixNano() / int64(time.Millisecond)) - starttime))
	return update, nil
}

/*
PublisheventBatchInSEQ - function to publish event to event engine
*/
func (processingUnit *coreProcessingUnit) PublisheventBatchInSEQ(kv map[string]string, seq map[string]string) (bool, error) {
	//generate key value to be stored in rocksdb and corresponding JSON data
	//key := strconv.FormatInt((time.Now().UnixNano() / int64(time.Millisecond)), 10)
	//store into Rocks DB
	starttime := time.Now().UnixNano() / int64(time.Millisecond)
	err := processingUnit.walstore.WriteBatchtoRocksDB(kv)
	glog.Infof("Time taken to save OmegaEvent in WAL: %d", ((time.Now().UnixNano() / int64(time.Millisecond)) - starttime))
	if err != nil {
		//need to log err and handle connection reset here
		//resetrocksdb()
		return false, err
	}

	for k := range kv {
		//publish to NATS
		starttime = time.Now().UnixNano() / int64(time.Millisecond)
		err = processingUnit.publisher.PublishtoqueuegroupInSeq(kv[k], seq[k])
		glog.Infof("Time taken to publish one OmegaEvent in QG: %d", ((time.Now().UnixNano() / int64(time.Millisecond)) - starttime))
		if err != nil {
			//need to log err and handle connection reset here
			//resetNATS()
			return false, err
		}
	}
	//starttime = time.Now().UnixNano() / int64(time.Millisecond)
	update := processingUnit.throttler.UpdateEventPublishedAndThrottel(int64(len(kv)))
	//glog.Infof("Time taken to update the throteller for one event: %d", ((time.Now().UnixNano() / int64(time.Millisecond)) - starttime))
	return update, nil
}

//HoldFlushAllEventsFromWAL -- function used to flush all events from WAL
func (engine *EventEngine) HoldFlushAllEventsFromWAL() {
	//get processingUnits for the existing Route
	var currentCoreEngine coreEngine
	for core := range engine.cores {
		currentCoreEngine = engine.cores[core]
		for processingUnit := range currentCoreEngine.processingUnit {
			coreProcess := currentCoreEngine.processingUnit[processingUnit]
			keepwaiting := true
			for keepwaiting {
				_, count, err := coreProcess.walstore.ReadRocksDBEntriesWithcount(10)
				if err != nil {
					glog.Infof("Failed retrieving events from WAL...", err)
					keepwaiting = false
				} else {
					if count != 0 {
						//sleep here for 10 seconds
						time.Sleep(10 * time.Second)
					} else {
						keepwaiting = false
					}
				}
			}
		}
	}
	glog.Infoln("Flush from WAL completed successfully")
}

//Start - function to start event engine
func (engine *EventEngine) Start(parseCB func(kv RocksData, partitionkey uint64, partitionsize uint64, partitionlength uint64) (string, int64, string)) {
	//get processingUnits for the existing Route
	var currentCoreEngine coreEngine
	for core := range engine.cores {
		currentCoreEngine = engine.cores[core]
		for processingUnit := range currentCoreEngine.processingUnit {
			coreProcess := currentCoreEngine.processingUnit[processingUnit]
			if coreProcess.publisher != nil {
				coreProcess.SetEventParserForKeySnapshot(parseCB)
				//Set ack subscription for events
				coreProcess.startACKSubscription(engine)
				//resend all the pending events first
				glog.Infoln("Waiting for processor to subscriber to be set...")
				<-coreProcess.subscriberset
				close(coreProcess.subscriberset)
				glog.Infoln("Subscriber set. Ready to proceed.")
				err := coreProcess.releasePendingEvents(engine)
				if err != nil {
					glog.Errorf("Error releasing pending events from event engine. Error occurred: %s\n", err)
					panic("not able to retrieve pending events.")
				}
				//start profiling the process here

				if len(coreProcess.profiler.Filename) > 2 {
					glog.Infof("Profiler file path set: %s of length: %d\n", coreProcess.profiler.Filename, len(coreProcess.profiler.Filename))
					go coreProcess.profiler.StartCPUProfiling()
					go coreProcess.profiler.StartMemoryProfiling()
				} else {
					glog.Infoln("Profiler is disabled for this engine..")
				}

				//start SLA Ticker for evnets
				go coreProcess.throttler.StartSLATicker(coreProcess.GetEventsFromLocalPersistance, coreProcess.PublisheventNoThrottel, coreProcess.parseEventRef)
			}
		}
	}
}

//StartNatsEventingSubscriptions - function used to start NATS ack eventing based processing --> this one is modelled based on NATS event messaging
func (engine *EventEngine) StartNatsEventingSubscriptions(publishCB func()) {
	defer func() {
		glog.Flush()
	}()
	//nothing can be done here apart from subscribing and processing
	publishCB()
}

//StartPublishAndThrottel - function to start publishing and Throttel --> This one is modelled for polling
func (engine *EventEngine) StartPublishAndThrottel(publishCB func(engine *EventEngine) error) {
	defer func() {
		glog.Flush()
	}()
	//code to get events published to mutiple publishers with throtteling capabilities.
	if atomic.LoadInt32(&engine.overloaded) == 1 {
		glog.Infoln("Received notification that the process got overloaded. Holding up to start event publisher till the load gets lighter.")
		engine.HoldOnLoad()
	}
	glog.Infoln("Starting event publisher.")
	//This is ending up with a funnel and am not sure how to throttel the publisher here.
	var count int64
	count = 0
	for engine.keeppublishing {
		starttime := time.Now().UnixNano() / int64(time.Millisecond)
		publishCB(engine)
		glog.Infof("Time taken to dequeue and publish: %d", ((time.Now().UnixNano() / int64(time.Millisecond)) - starttime))
		count++
		if (count * 10) > engine.EventEngineConfig.EngineThrottel.Maxpending {
			engine.HoldOnLoad()
			count = 0
		}
	}
	glog.Infof("Exiting out of start publish and not sure why? Current publishing status %b", engine.keeppublishing)
}

//HoldOnLoad - function to hold engine on overload
func (engine *EventEngine) HoldOnLoad() {
	if atomic.LoadInt32(&engine.overloaded) == 1 {
		glog.Infoln("Received notification that the process got overloaded. Holding up event publisher.")
		engine.cond.L.Lock()
		//Identify delayed overload call and by pass wait
		if engine.overloaded == 1 {
			engine.cond.Wait()
		}
		//engine.overloaded = 0 //removed locking for this as we are using lock here.
		engine.cond.L.Unlock()
		//releaseload := atomic.CompareAndSwapInt32(&engine.overloaded, 1, 0)
		glog.Infoln("Hold release on event publisher.")
	}
}

//NotifyOnLoad - function to notify engine on overload
func (engine *EventEngine) NotifyOnLoad() {
	loaded := atomic.CompareAndSwapInt32(&engine.overloaded, 0, 1)
	if loaded {
		glog.Infoln("Received notification that the process got overloaded. Got to hold the publisher for now.")
	}
}

//ReleaseLoad - function to release load from publisher
func (engine *EventEngine) ReleaseLoad() {
	glog.Infoln("Release hold for event engine called.")
	if atomic.CompareAndSwapInt32(&engine.overloaded, 1, 0) {
		glog.Infoln("Received notification that the process got freed up. Got notification to release the load now.")
		engine.cond.Signal()
		glog.Infoln("Signal provided to release hold.")
	}
}

//PublishBatch - function to publish events from events engine. The events are provided to engine in batch
func (engine *EventEngine) PublishBatch(kv map[string]string, seq map[string]string) error {
	//Get all cores and the corresponding stagenames use the stagenames to get processing unit and publish event to them
	for i := range engine.cores {
		glog.Infof("Current stages to which events are to be published: %s\n", engine.cores[i].name)
		processingUnit := engine.cores[i].processingUnit[engine.cores[i].name]
		if (processingUnit.insequence) && (len(seq) > 0) {
			overloaded, err := processingUnit.PublisheventBatchInSEQ(kv, seq)
			if err != nil {
				glog.Errorf("Error occured while publishing events to stage with name: %s\n", engine.cores[i].name)
				return err
			}
			if overloaded {
				engine.NotifyOnLoad()
			}
		} else {
			overloaded, err := processingUnit.PublisheventBatch(kv)
			if err != nil {
				glog.Errorf("Error occured while publishing events to stage with name: %s\n", engine.cores[i].name)
				return err
			}
			if overloaded {
				engine.NotifyOnLoad()
			}
		}
	}
	return nil
}

//Publish - function to publish events from event engine
func (engine *EventEngine) Publish(key string, events string, seq string) error {
	numberofcores := len(engine.cores)
	letgo := make(chan coreEngine, numberofcores)
	//Get all cores and the corresponding stagenames use the stagenames to get processing unit and publish event to them
	for i := range engine.cores {
		go func(i int) {
			engine.cores[i].processerror = nil
			glog.Infof("Current stages to which events are to be published: %s\n", engine.cores[i].name)
			processingUnit := engine.cores[i].processingUnit[engine.cores[i].name]
			if (processingUnit.insequence) && (len(seq) > 0) {
				overloaded, err := processingUnit.PublisheventInSEQ(key, events, seq)
				if err != nil {
					glog.Errorf("Error occured while publishing events to stage with name: %s\n", engine.cores[i].name)
					engine.cores[i].processerror = err
					//return err
				}
				if overloaded {
					engine.NotifyOnLoad()
				}
			} else {
				overloaded, err := processingUnit.Publishevent(key, events)
				if err != nil {
					glog.Errorf("Error occured while publishing events to stage with name: %s\n", engine.cores[i].name)
					engine.cores[i].processerror = err
				}
				if overloaded {
					engine.NotifyOnLoad()
				}
			}
			letgo <- engine.cores[i]
		}(i)
	}
	//close(letgo)
	var err error
	for j := 0; j < numberofcores; j++ {
		core := <-letgo
		glog.Infof("Publishing of data complete for core engine [%d]: %s", j, core.name)
		if core.processerror != nil {
			err = core.processerror
			glog.Errorf("Error Publishing of data from core engine[%s] with error: %s", core.name, err)
		}
	}
	close(letgo)
	return err
}

//PublishBatchAsync - function to publish events from events engine. The events are provided to engine in batch
func (engine *EventEngine) PublishBatchAsync(kv map[string]string, seq map[string]string) error {
	numberofcores := len(engine.cores)
	letgo := make(chan coreEngine, numberofcores)
	//Get all cores and the corresponding stagenames use the stagenames to get processing unit and publish event to them
	for i := range engine.cores {
		go func(i int) {
			engine.cores[i].processerror = nil
			glog.Infof("Current stages to which events are to be published: %s\n", engine.cores[i].name)
			processingUnit := engine.cores[i].processingUnit[engine.cores[i].name]
			if (processingUnit.insequence) && (len(seq) > 0) {
				overloaded, err := processingUnit.PublisheventBatchInSEQ(kv, seq)
				if err != nil {
					glog.Errorf("Error occured while publishing events to stage with name: %s\n", engine.cores[i].name)
					engine.cores[i].processerror = err
					//return err
				}
				if overloaded {
					engine.NotifyOnLoad()
				}
			} else {
				overloaded, err := processingUnit.PublisheventBatch(kv)
				if err != nil {
					glog.Errorf("Error occured while publishing events to stage with name: %s\n", engine.cores[i].name)
					engine.cores[i].processerror = err
				}
				if overloaded {
					engine.NotifyOnLoad()
				}
			}
			letgo <- engine.cores[i]
		}(i)
	}
	//close(letgo)
	var err error
	for j := 0; j < numberofcores; j++ {
		core := <-letgo
		glog.Infof("Publishing of data complete for core engine [%d]: %s", j, core.name)
		if core.processerror != nil {
			err = core.processerror
			glog.Errorf("Error Publishing of data from core engine[%s] with error: %s", core.name, err)
		}
	}
	close(letgo)
	return err
}
