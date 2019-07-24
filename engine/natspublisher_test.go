package engine

import (
  "fmt"
  "testing"
)

func getNatsPublisher() *NatsPublisher {
  npub := new(NatsPublisher)
  return npub
}

func startNatsServer() *NatsCoreServer {
  ns := new(NatsCoreServer)
  if ns.StartNatsCore(){
    url, _ := ns.GetConnectionURL()
    fmt.Printf("Nats started with url: %s\n", url)
    return ns
  } else {
    fmt.Println("Failed starting nats server")
  }
  return nil
}

func startNatsServerSecure() *NatsCoreServer {
  ns := new(NatsCoreServer)
  ns.Certfile = "/home/jayaprakash/certs/server/astraserver.pem"
  ns.Keyfile = "/home/jayaprakash/certs/server/astrakey.pem"
  ns.CAfile = "/home/jayaprakash/certs/server/ca.pem"
  status, err := ns.StartNatsCoreSecure()
  if status {
    url, _ := ns.GetSecuredConnectionURL()
    fmt.Printf("Nats started with url: %s\n", url)
    return ns
  } else {
    fmt.Printf("Failed started nats server: %s\n", err)
  }
  return nil
}

func stopNatsServer(ns *NatsCoreServer){
  ns.StopNatsCore()
}

/*
func TestConnectNats(t *testing.T){
  ns := startNatsServer()
  defer stopNatsServer(ns)
  npub := getNatsPublisher()
  err := npub.ConnectNats()
  if err == nil {
    t.Fatalf("Able to connect without NATSUrl")
  }
  npub.Defaulturls = "nats://astra:astrarocks!10_0_2_15@10.0.2.15:4242"
  err = npub.ConnectNats()
  if err != nil {
    t.Fatalf("Failed to connect to NATSUrl with default urls")
  }
}
*/


func TestSecureConnectNats(t *testing.T){
  ns := startNatsServerSecure()
  defer stopNatsServer(ns)
  npub := getNatsPublisher()
  npub.Defaulturls = "tls://ps3cat5505k1.us.dell.com:4242"
  npub.Certfile = "/home/jayaprakash/certs/client/astraclient.pem"
  npub.Keyfile = "/home/jayaprakash/certs/client/astrakey.pem"
  npub.Cafile = "/home/jayaprakash/certs/client/ca.pem"
  err := npub.SecureConnectNats(true)
  if err != nil {
    t.Fatalf("Failed to connect to NATSUrl with secure urls: %s\n", err)
  }
}
