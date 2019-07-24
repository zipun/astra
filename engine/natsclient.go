package engine

import (
  "crypto/tls"
  "crypto/x509"
  "io/ioutil"
  "errors"

	glog "github.com/golang/glog"
	nats "github.com/nats-io/nats.go"
)

//NatsClient - NatsClient used to make and manage NATS Connection
type NatsClient struct {
	Defaulturls      string
	Natsconn         *nats.Conn
	Name             string
	Certfile         string
	Keyfile 			   string
	Cafile				   string
  AuthorizeClient  bool
}

//MakeConnection - create a non secured NATS Connection
func (n *NatsClient) MakeConnection() error {
  if len(n.Defaulturls) <=0 {
		return errors.New("Nats url is required for making connection")
	}
  var err error
	n.Natsconn,err = nats.Connect(n.Defaulturls)
	if err != nil {
		glog.Errorf("Error connecting to NATS server. Error: %s\n", err)
	}
	return err
}

//MakeGenericTLSConnection - function to create generic TLS connection with no authorization
func (n *NatsClient) MakeGenericTLSConnection() error {
  if len(n.Defaulturls) <=0 {
		return errors.New("Nats url is required for making connection")
	}
  if len(n.Cafile) <= 0 {
    return errors.New("RootCA certificate not found. Cannot create TLS connection from client")
  }
  opt := nats.RootCAs(n.Cafile)
  var err error
  n.Natsconn, err = nats.Connect(n.Defaulturls, opt)
  if err != nil {
    glog.Errorf("Error connecting to NATS using tls. Err: %s\n", err)
  }
  return err
}

//MakeAuthorizedTLSConnection - function to create authorized connection with Client using TLS
func (n *NatsClient) MakeAuthorizedTLSConnection() error {
  if len(n.Defaulturls) <=0 {
		return errors.New("Nats url is required for making connection")
	}
  if (len(n.Certfile)<=0) || (len(n.Keyfile)<=0){
		return errors.New("Certificate files not found, hence cannot start NATS Secure")
	}

  cert, err := tls.LoadX509KeyPair(n.Certfile, n.Keyfile)
  if err != nil {
    glog.Errorf("Loading X509 certificate files. Err: %s", err)
  }
  rootPEM, err := ioutil.ReadFile(n.Cafile)
	if err != nil || rootPEM == nil {
		glog.Errorf("failed to read root certificate")
	}

  pool := x509.NewCertPool()
	ok := pool.AppendCertsFromPEM([]byte(rootPEM))
	if !ok {
		glog.Errorf("failed to parse root certificate")
  }
  config := &tls.Config{
      Certificates: 	[]tls.Certificate{cert},
      RootCAs:    	pool,
      MinVersion: 	tls.VersionTLS12,
  }
  n.Natsconn, err = nats.Connect(n.Defaulturls, nats.Secure(config))
  if err != nil {
    glog.Errorf("Error connecting to NATS using tls. Err: %s\n", err)
  }
  return err
}
