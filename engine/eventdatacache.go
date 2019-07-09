package engine

import "sync/atomic"

/*
Eventlocalcache - used to hold data in memory temporarly with no persistance.
This is a simple store to support write and reads.
Read will remove data from cache which is similar to delete.
Trying this implementation on lock-free linked lists.
*/
type Eventlocalcache struct {
	name          string
	eventstore    *eventpool
	usereadblocks bool
	waitswitch    int32
	rwait         chan bool
}

type eventpool struct {
	headref *eventnode
	tailref *eventnode
	length  int64
}

/*
eventnode - used to set one event as a node in cache
*/
type eventnode struct {
	data      string
	head      *eventnode
	tail      *eventnode
	isrefnode bool
}

/*
Neweventcache - used to get new event cache instance
*/
func Neweventcache(name string, usereadblocks bool) *Eventlocalcache {
	cache := new(Eventlocalcache)
	cache.name = name
	cache.eventstore = new(eventpool)
	cache.eventstore.length = 0
	cache.eventstore.headref = getrefeventnode()
	cache.eventstore.headref.data = "headref"
	cache.eventstore.tailref = getrefeventnode()
	cache.eventstore.tailref.data = "tailref"
	cache.eventstore.headref.head = cache.eventstore.headref
	cache.eventstore.headref.tail = cache.eventstore.tailref
	cache.eventstore.tailref.head = cache.eventstore.headref
	cache.eventstore.tailref.tail = cache.eventstore.tailref
	cache.usereadblocks = usereadblocks
	cache.rwait = make(chan bool, 1)
	cache.waitswitch = 0
	return cache
}

/*
Putevent - write event to cache
*/
func (cachehandler *Eventlocalcache) Putevent(event string) {
	prevnode := cachehandler.eventstore.tailref.head
	node := getneweventnode()
	node.data = event
	node.head = prevnode
	node.tail = cachehandler.eventstore.tailref
	cachehandler.eventstore.tailref.head = node
	prevnode.tail = node
	/* disabling length to improve performance
	atomic.AddInt64(&cachehandler.eventstore.length, 1)
	*/
	if cachehandler.waitswitch == 1 { //going for dirty read here intentionally
		cachehandler.eventwritesignal()
	}
}

/*
Geteventblockingcall - read data from cache
*/
func (cachehandler *Eventlocalcache) Geteventblockingcall() string {
	if cachehandler.eventstore.headref.tail.isrefnode {
		cachehandler.eventreadwait()
	}
	value := cachehandler.eventstore.headref.tail.data
	cachehandler.resethref(cachehandler.eventstore.headref.tail)
	/* disabling length queries for improving performance
	if atomic.LoadInt64(&cachehandler.eventstore.length) > 0 {
		atomic.AddInt64(&cachehandler.eventstore.length, -1)
	}
	*/
	return value
}

//resethref - reset href to other node
func (cachehandler *Eventlocalcache) resethref(node *eventnode) {
	cachehandler.eventstore.headref.head = nil
	cachehandler.eventstore.headref.tail = nil
	cachehandler.eventstore.headref = node
	cachehandler.eventstore.headref.isrefnode = true
	cachehandler.eventstore.headref.data = "headref"
	cachehandler.eventstore.headref.head = cachehandler.eventstore.headref
}

/*
Getevent - read data from cache
*/
func (cachehandler *Eventlocalcache) Getevent() string {
	if cachehandler.eventstore.headref.tail.isrefnode {
		return ""
	}
	value := cachehandler.eventstore.headref.tail.data
	cachehandler.resethref(cachehandler.eventstore.headref.tail)
	/* disabling length queries for improving performance
	if atomic.LoadInt64(&cachehandler.eventstore.length) > 0 {
		atomic.AddInt64(&cachehandler.eventstore.length, -1)
	}
	*/
	return value
}

/*
Getcachesize - get size of data cache - going for disabled mode

func (cachehandler *Eventlocalcache) Getcachesize() int64 {
	return atomic.LoadInt64(&cachehandler.eventstore.length)
}
*/

func (cachehandler *Eventlocalcache) eventwritesignal() {
	if atomic.CompareAndSwapInt32(&cachehandler.waitswitch, 1, 0) {
		cachehandler.rwait <- true
	}
}

func (cachehandler *Eventlocalcache) eventreadwait() {
	if atomic.CompareAndSwapInt32(&cachehandler.waitswitch, 0, 1) {
		<-cachehandler.rwait
	}
}

/*
Release - used to release event cache instance
*/
func (cachehandler *Eventlocalcache) Release() {
	for cachehandler.Getevent() != "" {
	}
	cachehandler.eventstore.headref.head = nil
	cachehandler.eventstore.headref.tail = nil
	cachehandler.eventstore.headref = nil
	cachehandler.eventstore.tailref.head = nil
	cachehandler.eventstore.tailref.tail = nil
	cachehandler.eventstore.tailref = nil
	close(cachehandler.rwait)
	cachehandler.rwait = nil
	cachehandler.eventstore = nil
}

func getneweventnode() *eventnode {
	node := new(eventnode)
	node.head = nil
	node.tail = nil
	node.isrefnode = false
	node.data = ""
	return node
}

func getrefeventnode() *eventnode {
	node := new(eventnode)
	node.head = nil
	node.tail = nil
	node.data = ""
	node.isrefnode = true
	return node
}
