package engine

import (
	"sync/atomic"
	"time"

	glog "github.com/glog-master"
)

//Throttler - structure to create throttler for event engine
type Throttler struct {
	Name               string
	Maxpending         int64
	SLAInMillisec      int64
	SLAcount           int
	Error              error
	stopTickerChan     chan bool
	currentEventsCount int64
	PartitionSize      uint64
	PartitionPosition  uint64
	PartitionLength    uint64
}

//CreateEngineThrotteler - create event engine throttler
func (engineThrottel *Throttler) CreateEngineThrotteler() {
	atomic.StoreInt64(&engineThrottel.currentEventsCount, 0)
}

/*
GetThrottelerEventCount - Get current pending count from event processor
*/
func (engineThrottel *Throttler) GetThrottelerEventCount() int64 {
	return atomic.LoadInt64(&engineThrottel.currentEventsCount)
}

/*
UpdateACKAndThrottel - function to throttel on ACK event
*/
func (engineThrottel *Throttler) UpdateACKAndThrottel() bool {
	if engineThrottel.GetThrottelerEventCount() > 0 {
		atomic.AddInt64(&engineThrottel.currentEventsCount, -1)
		glog.Infof("Current pending count: %d and max pending set: %d", engineThrottel.GetThrottelerEventCount(), engineThrottel.Maxpending)
		if engineThrottel.GetThrottelerEventCount() < engineThrottel.Maxpending {
			//engineThrottel.cond.Signal()
			return true
		}
	}
	return false
}

/*
UpdateACKsAndThrottel - function to throttel on ACK event
*/
func (engineThrottel *Throttler) UpdateACKsAndThrottel(count int64) bool {
	if engineThrottel.GetThrottelerEventCount() > 0 {
		atomic.AddInt64(&engineThrottel.currentEventsCount, int64(^uint64(count-1)))
		glog.Infof("Current pending count: %d and max pending set: %d", engineThrottel.GetThrottelerEventCount(), engineThrottel.Maxpending)
		if engineThrottel.GetThrottelerEventCount() < engineThrottel.Maxpending {
			//engineThrottel.cond.Signal()
			return true
		}
	}
	return false
}

/*
UpdateEventPublishedAndThrottel - function to throttel on event published
*/
func (engineThrottel *Throttler) UpdateEventPublishedAndThrottel(count int64) bool {
	atomic.AddInt64(&engineThrottel.currentEventsCount, count)
	if engineThrottel.GetThrottelerEventCount() > engineThrottel.Maxpending {
		return true
	}
	return false
}

/*
StartSLATicker - function to start SLA Ticker
Need to be careful with this.. have to optimized for multiple executions
Need to come back on this for furthen sync handling related issues.
*/
func (engineThrottel *Throttler) StartSLATicker(getEventsFromLocalPersistance func(count int) (map[int64]string, error),
	publishNothrottel func(event string, seq string) error,
	parseEventRef func(kv RocksData, partitionkey uint64, partitionsize uint64, partitionlength uint64) (string, int64, string)) {
	engineThrottel.stopTickerChan = make(chan bool)
	glog.Infof("Starting SLA Ticker set @ %d milliseconds", engineThrottel.SLAInMillisec)
	tickerChannel := time.NewTicker(time.Duration(engineThrottel.SLAInMillisec) * time.Millisecond).C
	for {
		select {
		case <-tickerChannel:
			go func() {
				keys, err := getEventsFromLocalPersistance(engineThrottel.SLAcount)
				if err != nil {
					glog.Errorf("SLA Ticker failed to get event messages: %s", err)
				}
				for key := range keys {
					currentMSec := (time.Now().UnixNano() / int64(time.Millisecond))
					if currentMSec-key >= engineThrottel.SLAInMillisec {
						kv := RocksData{keys[key]}
						keyid, _, seq := parseEventRef(kv, engineThrottel.PartitionSize, engineThrottel.PartitionPosition, engineThrottel.PartitionLength)
						error := publishNothrottel(keys[key], seq)
						kv.Event = ""
						if error != nil {
							glog.Errorf("SLA Ticker failed publishing message: %s", keys[key])
						} else {
							glog.Infof("Event Published by SLA Tracker [%s]: %s \n value: %s", engineThrottel.Name, keyid, keys[key])
						}
					}
				}
			}()
		case <-engineThrottel.stopTickerChan:
			return
		}
	}
}

/*
StopSLATicker - function to stop SLA ticker
*/
func (engineThrottel *Throttler) StopSLATicker() {
	if engineThrottel.stopTickerChan != nil {
		engineThrottel.stopTickerChan <- true
	}
}
