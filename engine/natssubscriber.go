package engine

import (
	"errors"

	glog "github.com/glog-master"
	nats "github.com/nats-io/nats"
)

//NatsSubscriber - structure for creating nats subscriber instance
type NatsSubscriber struct {
	Defaulturls    string
	Subject        string
	Natsconn       *nats.Conn
	Configfilepath *string
	Natserr        error
	Name           string
	QueueGroupName string
}

/*
ConnectNatsAckSubscriber - function used to connect to NATS cluster
*/
func (subscriber *NatsSubscriber) ConnectNatsAckSubscriber() error {
	subscriber.Natsconn, subscriber.Natserr = nats.Connect(subscriber.Defaulturls)
	if subscriber.Natserr != nil {
		glog.Errorf("Error connecting to NATS server. Error: %s\n", subscriber.Natserr)
	}
	return subscriber.Natserr
}

/*
SubscribeACKs - function to subscribeACKS after message being processed by subscribers
*/
func (subscriber *NatsSubscriber) SubscribeACKs(callbk func(string)) {
	if subscriber.Natsconn != nil {
		subscriber.Natsconn.Subscribe(subscriber.Subject, func(msg *nats.Msg) {
			callbk(string(msg.Data[:len(msg.Data)]))
		})
		glog.Infof("ACK subscription started for event engine. Subscribed for %s\n", subscriber.Subject)
	}
}

/*
SubscribeQueueGroupACKs - function to subscribeACKS after message being processed by subscribers
*/
func (subscriber *NatsSubscriber) SubscribeQueueGroupACKs(callbk func(string)) {
	if subscriber.Natsconn != nil {
		subscriber.Natsconn.QueueSubscribe(subscriber.Subject, subscriber.QueueGroupName, func(msg *nats.Msg) {
			callbk(string(msg.Data[:len(msg.Data)]))
		})
		glog.Infof("ACK subscription started for event engine. Subscribed for %s\n", subscriber.Subject)
	}
}

/*
CloseNatsSubscriber - function used to disconnect from NATS cluster
*/
func (subscriber *NatsSubscriber) CloseNatsSubscriber() error {
	if subscriber.Natsconn != nil {
		subscriber.Natsconn.Close()
		return nil
	}
	return errors.New("nats connection object has invalid reference to close the connection")
}
