package main

import (
  "fmt"
  "sync"
  "os"
  e "github.com/jayaprakash-j/astra/engine"
)

func main() {
  var wg sync.WaitGroup
  wg.Add(1)
  //ns := startNatsServer()
  ns := startNatsServerSecure()
  defer stopNatsServer(ns)
  if ns == nil{
    fmt.Printf("failed to start NATS server")
    os.Exit(0)
  }
  //url, err := ns.GetConnectionURL()
  url, err := ns.GetSecuredConnectionURL()
  if err != nil {
    fmt.Printf("Error retrieving connection url from NATS: %s", err)
    os.Exit(0)
  }
  fmt.Printf("Nats server running with url: %s", url)
  wg.Wait()
}

func startNatsServer() *e.NatsCoreServer{
  ns := new(e.NatsCoreServer)
  if ns.StartNatsCore(){
    return ns
  } else {
    fmt.Println("Failed starting nats server")
  }
  return nil
}

func startNatsServerSecure() *e.NatsCoreServer {
  ns := new(e.NatsCoreServer)
  ns.Certfile = "/home/jayaprakash/certs/server/astraserver.pem"
  ns.Keyfile = "/home/jayaprakash/certs/server/astrakey.pem"
  ns.CAfile = "/home/jayaprakash/certs/server/ca.pem"
  status, err := ns.StartNatsCoreSecure()
  if status {
    return ns
  } else {
    fmt.Printf("Failed started nats server: %s", err)
  }
  return nil
}

func stopNatsServer(ns *e.NatsCoreServer){
  ns.StopNatsCore()
}
