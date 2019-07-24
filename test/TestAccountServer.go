package main

import (
  "fmt"
  "sync"
  "strconv"
  "os"
  e "github.com/jayaprakash-j/astra/engine"
)

func main() {
  var wg sync.WaitGroup
  wg.Add(1)
  //start nats server
  n := startNatsServer()
  defer n.StopNatsCore()
  url, err := n.GetConnectionURL()
  if err != nil{
    fmt.Printf("Error getting connection url from nats: %s", err)
    os.Exit(0)
  }
  fmt.Printf("Connection url for nats: %s", url)
  //Start account server
  a := startAccountingServer(url)
  defer a.Stop()
  wg.Wait()
}

func startNatsServer() *e.NatsCoreServer{
  n := new(e.NatsCoreServer)
  s := n.StartNatsCore()
  fmt.Printf("Nats server started: %s", strconv.FormatBool(s))
  return n
}

func startAccountingServer(natsurl string) *e.AccountServer{
  a := new(e.AccountServer)
  a.Hostport = "10.0.2.15:5990"
  a.NSCFolder = "/home/jayaprakash/astra_v2/github.com/jayaprakash-j/astra/nscfolder/nats/astra"
  a.NATSUrl = natsurl
  a.Verbose = true
  a.Initialize()
  a.Start()
  return a
}
