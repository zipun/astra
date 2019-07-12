package engine

import (
	"errors"
	"time"

	glog "github.com/golang/glog"
	nats "github.com/nats-io/nats.go"
)

//NatsPublisher - structure for creating nats publisher instance
type NatsPublisher struct {
	Defaulturls    string
	Subject        string
	Queuegroup     string
	Natsconn       *nats.Conn
	Configfilepath *string
	Natserr        error
	Name           string
}

/*
ConnectNats - function used to connect to NATS cluster
*/
func (publisher *NatsPublisher) ConnectNats() error {
	publisher.Natsconn, publisher.Natserr = nats.Connect(publisher.Defaulturls)
	if publisher.Natserr != nil {
		glog.Errorf("Error connecting to NATS server. Error: %s\n", publisher.Natserr)
	}
	return publisher.Natserr
}

/*
ConnectNatsBlockingLooper - function used to connect to NATS cluster
*/
func (publisher *NatsPublisher) ConnectNatsBlockingLooper() error {
	keeptrying := true
	for keeptrying {
		publisher.Natsconn, publisher.Natserr = nats.Connect(publisher.Defaulturls)
		if publisher.Natserr != nil {
			glog.Errorf("Error connecting to NATS server. Error: %s\n", publisher.Natserr)
			time.Sleep(time.Second * 10)
		} else {
			keeptrying = false
		}
	}
	return publisher.Natserr
}

/*
Publishtoqueuegroup - function to publish to queue group on a NATS cluster
*/
func (publisher *NatsPublisher) Publishtoqueuegroup(data string) error {
	if publisher.Natsconn != nil {
		publisher.Natserr = publisher.Natsconn.Publish(publisher.Subject, []byte(data))
		if publisher.Natserr != nil {
			return publisher.Natserr
		}
		//glog.Infof("Flushing event to server: %s\n", data)
		/*
			publisher.Natserr = publisher.Natsconn.Flush()
			if publisher.Natserr != nil {
				return publisher.Natserr
			}
		*/
		return nil
	}
	return errors.New("failed to publish into queuegroup as the connection object has invalid reference")
}

/*
PublishtoqueuegroupInSeq - function to publish to queue group on a NATS cluster
*/
func (publisher *NatsPublisher) PublishtoqueuegroupInSeq(data string, seq string) error {
	if publisher.Natsconn != nil {
		subject := []byte(publisher.Subject)
		seqid := []byte(seq)
		for c := range seqid {
			subject = append(subject, seqid[c])
		}
		starttime := time.Now().UnixNano() / int64(time.Millisecond)
		publisher.Natserr = publisher.Natsconn.Publish(string(subject), []byte(data))
		glog.Infof("Time taken to publish one OmegaEvent to NATS PUB[%s]: %d", string(subject), ((time.Now().UnixNano() / int64(time.Millisecond)) - starttime))
		if publisher.Natserr != nil {
			return publisher.Natserr
		}
		//glog.Infof("Flushing event to server: %s\n", data)
		/*  Commented by JP as part of performance tuning
		publisher.Natserr = publisher.Natsconn.Flush()
		glog.Infof("Time taken to Flush one OmegaEvent to NATS PUB: %d", ((time.Now().UnixNano() / int64(time.Millisecond)) - starttime))
		if publisher.Natserr != nil {
			return publisher.Natserr
		}
		*/
		return nil
	}
	return errors.New("failed to publish into queuegroup as the connection object has invalid reference")
}

/*
CloseNats - function used to disconnect from NATS cluster
*/
func (publisher *NatsPublisher) CloseNats() error {
	if publisher.Natsconn != nil {
		publisher.Natsconn.Close()
		return nil
	}
	return errors.New("nats connection object has invalid reference to close the connection")
}
