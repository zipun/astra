package main

import (
  "crypto/tls"
  "crypto/x509"
  "io/ioutil"
  "fmt"
  nats "github.com/nats-io/nats.go"
)

func main(){
  certfile := "/home/jayaprakash/certs/client/astraclient.pem"
  keyfile := "/home/jayaprakash/certs/client/astrakey.pem"
  cafile := "/home/jayaprakash/certs/client/ca.pem"
  url := "tls://ps3cat5505k1.us.dell.com:4242"

  cert, err := tls.LoadX509KeyPair(certfile, keyfile)
  if err != nil {
      fmt.Errorf("error parsing X509 certificate/key pair: %v", err)
  }
  rootPEM, err := ioutil.ReadFile(cafile)
  	if err != nil || rootPEM == nil {
  		fmt.Errorf("failed to read root certificate")
  	}
  	pool := x509.NewCertPool()
  	ok := pool.AppendCertsFromPEM([]byte(rootPEM))
  	if !ok {
  		fmt.Errorf("failed to parse root certificate")
  }
  config := &tls.Config{
      ServerName: 	"ps3cat5505k1.us.dell.com",
      Certificates: 	[]tls.Certificate{cert},
      RootCAs:    	pool,
      MinVersion: 	tls.VersionTLS12,
  }

  _, err = nats.Connect(url, nats.Secure(config))
  //_, err := nats.Connect(url)
  if err != nil {
    fmt.Printf("Error connecting to nats server: %s", err)
  } else {
    fmt.Printf("Connection to Nats successfully")
  }

}
