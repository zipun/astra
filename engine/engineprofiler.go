package engine

import (
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"time"

	http "net/http"
	/*
		To include default pprof handlers to be used for profiling
		Then use the pprof tool to look at the heap profile:
		go tool pprof http://localhost:6060/debug/pprof/heap

		Or to look at a 30-second CPU profile:
		go tool pprof http://localhost:6060/debug/pprof/profile

		Or to look at the goroutine blocking profile, after calling runtime.SetBlockProfileRate in your program:
		go tool pprof http://localhost:6060/debug/pprof/block

		Or to collect a 5-second execution trace:
		wget http://localhost:6060/debug/pprof/trace?seconds=5
	*/
	_ "net/http/pprof"

	glog "github.com/glog-master"
)

//Profiler - struct used to schedule profiling for the event engine
type Profiler struct {
	Filename     string
	Scheduledfor int64
	name         string
	cpu          *cpuProfiler
	memory       *memoryProfiler
}

//cpuProfiler - struct used to schedule CPU profiling
type cpuProfiler struct {
	currentcpufname string
	timeTicker      <-chan time.Time
	done            chan bool
	f               *os.File
	err             error
}

//memoryProfiler - struct used to schedule memory profiling
type memoryProfiler struct {
	currentmemfname string
	timeTicker      <-chan time.Time
	done            chan bool
	f               *os.File
	err             error
}

//RestProfiler - struct used to start REST based profiling
type RestProfiler struct {
	Httpaddr string
}

//StartRestProfiler - function used to enable webbased profiling for event engine
func (httpprofiler *RestProfiler) StartRestProfiler() {
	go func() {
		glog.Infof("Starting default profiler at the address: %s \n", httpprofiler.Httpaddr)
		http.ListenAndServe(httpprofiler.Httpaddr, nil)
	}()
}

//CreateProfiler - function used to initialize profiler for event engine
func CreateProfiler(cpuprofile string, scheduledFor int64, name string) *Profiler {
	profiler := new(Profiler)
	if cpuprofile != "" {
		profiler.Filename = cpuprofile
		profiler.Scheduledfor = scheduledFor
		profiler.name = name
		profiler.cpu = new(cpuProfiler)
		profiler.memory = new(memoryProfiler)
		glog.Infof("Profiler initialized for event engine [%s]", name)
	} else {
		glog.Warningf("Could not initialized profiler for the event engine [%s]", name)
	}
	return profiler
}

//StartCPUProfiling - function used to start profiler for event engine
func (profiler *Profiler) StartCPUProfiling() {
	glog.Infof("Starting cpu profiler [%s]", profiler.name)
	profiler.cpu.timeTicker = time.NewTicker(time.Duration(profiler.Scheduledfor) * time.Minute).C
	profiler.cpu.done = make(chan bool)
	for {
		select {
		case <-profiler.cpu.timeTicker:
			profiler.stopCPUProfiler()
			profiler.getCPUfilenames()
			err := profiler.startCPUProfiler()
			if err != nil {
				return
			}
		case <-profiler.cpu.done:
			return
		}
	}
}

//StopCPUProfiling - function used to stop profiler for event engine
func (profiler *Profiler) StopCPUProfiling() {
	profiler.cpu.done <- true
}

//startCPUProfiler - function to start CPU Profile
func (profiler *Profiler) startCPUProfiler() error {
	profiler.cpu.f, profiler.cpu.err = os.Create(profiler.cpu.currentcpufname)
	if profiler.cpu.err != nil {
		glog.Errorf("Cannot start profiling for this run. Failed with error: %s", profiler.cpu.err)
		return profiler.cpu.err
	}
	glog.Infof("CPU profiling filepath set %s\n", profiler.cpu.f.Name())
	pprof.StartCPUProfile(profiler.cpu.f)
	return nil
}

//stopCPUProfiler - function to stop CPU Profile
func (profiler *Profiler) stopCPUProfiler() {
	if profiler.cpu.f != nil {
		pprof.StopCPUProfile()
		profiler.cpu.f.Close()
		profiler.cpu.f = nil
	}
}

//StartMemoryProfiling - function used to start profiler for event engine
func (profiler *Profiler) StartMemoryProfiling() {
	glog.Infof("Starting memory profiler [%s]", profiler.name)
	profiler.memory.timeTicker = time.NewTicker(time.Duration(profiler.Scheduledfor) * time.Minute).C
	profiler.memory.done = make(chan bool)
	for {
		select {
		case <-profiler.memory.timeTicker:
			profiler.stopMemoryProfiler()
			profiler.getMemfilenames()
			err := profiler.startMemoryProfiler()
			if err != nil {
				return
			}
		case <-profiler.memory.done:
			return
		}
	}
}

//StopMemoryProfiling - function used to stop profiler for event engine
func (profiler *Profiler) StopMemoryProfiling() {
	profiler.memory.done <- true
}

//startMemoryProfiler - function to start memory Profile
func (profiler *Profiler) startMemoryProfiler() error {
	profiler.memory.f, profiler.memory.err = os.Create(profiler.memory.currentmemfname)
	if profiler.memory.err != nil {
		glog.Errorf("Cannot start profiling for this run. Failed with error: %s", profiler.memory.err)
		return profiler.memory.err
	}
	glog.Infof("Memory profiling filepath set %s\n", profiler.memory.f.Name())
	glog.Infof("Current memory profiling rate set %d\n", runtime.MemProfileRate)
	return nil
}

//stopMemoryProfiler - function to stop memory Profile
func (profiler *Profiler) stopMemoryProfiler() {
	if profiler.memory.f != nil {
		profiler.memory.err = pprof.WriteHeapProfile(profiler.memory.f)
		if profiler.memory.err != nil {
			glog.Errorf("Error profiling memory for this run. Failed with error: %s", profiler.memory.err)
		}
		profiler.memory.f.Close()
		profiler.memory.f = nil
	}
}

//getCPUfilenames - function to create filename to store CPU profile
func (profiler *Profiler) getCPUfilenames() {
	cfpath := []byte(profiler.Filename)
	filename := []byte(profiler.name)
	ext := []byte(string(".prof"))
	inst := []byte(strconv.FormatInt((time.Now().UnixNano() / int64(time.Millisecond)), 10))
	for c := range inst {
		filename = append(filename, inst[c])
	}
	for c := range ext {
		filename = append(filename, ext[c])
	}
	for c := range filename {
		cfpath = append(cfpath, filename[c])
	}
	profiler.cpu.currentcpufname = string(cfpath)
}

//getMemfilenames - function to create filename to store CPU profile
func (profiler *Profiler) getMemfilenames() {
	mfpath := []byte(profiler.Filename)
	filename := []byte(profiler.name)
	mext := []byte(string(".mprof"))
	inst := []byte(strconv.FormatInt((time.Now().UnixNano() / int64(time.Millisecond)), 10))
	for c := range inst {
		filename = append(filename, inst[c])
	}
	for c := range mext {
		filename = append(filename, mext[c])
	}
	for c := range filename {
		mfpath = append(mfpath, filename[c])
	}
	profiler.memory.currentmemfname = string(mfpath)
}
