package engine

import (
  "fmt"
  "testing"
)

func getNatsClient() *NatsClient {
  return new(NatsClient)
}

func startLocalNatsServer() *NatsCoreServer {
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

func startLocalNatsServerSecure() *NatsCoreServer {
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

func stopLocalNatsServer(ns *NatsCoreServer){
  ns.StopNatsCore()
}

func TestMakeConnection(t *testing.T){
  nc := getNatsClient()
  ns := startLocalNatsServer()
  defer stopLocalNatsServer(ns)
  nc.Defaulturls, _ = ns.GetConnectionURL()
  err := nc.MakeConnection()
  if (err != nil) {
    t.Fatalf("Failed to make connection with default urls. Err: %s", err)
  }
}

func TestMakeGenericTLSConnection(t *testing.T){
  nc := getNatsClient()
  ns := startLocalNatsServerSecure()
  defer stopLocalNatsServer(ns)
  nc.Defaulturls, _ = ns.GetSecuredConnectionURL()
  nc.Cafile = "/home/jayaprakash/certs/client/ca.pem"
  err := nc.MakeGenericTLSConnection()
  if (err != nil){
    t.Fatalf("Failed to make generic TLS connection with default urls. Err: %s", err)
  }
}

func TestMakeAuthorizedTLSConnection(t *testing.T){
  nc := getNatsClient()
  ns := startLocalNatsServerSecure()
  defer stopLocalNatsServer(ns)
  nc.Defaulturls, _ = ns.GetSecuredConnectionURL()
  nc.Cafile = "/home/jayaprakash/certs/client/ca.pem"
  nc.Certfile = "/home/jayaprakash/certs/client/astraclient.pem"
  nc.Keyfile = "/home/jayaprakash/certs/client/astrakey.pem"
  err := nc.MakeAuthorizedTLSConnection()
  if (err != nil){
    t.Fatalf("Failed to make generic TLS connection with default urls. Err: %s", err)
  }
}
