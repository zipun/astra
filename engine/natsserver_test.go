package engine

import (
    "fmt"
    "testing"
  )

func getNatsServerv2() *NatsCoreServer {
  return new(NatsCoreServer)
}

//Testing nats core with default options to start a server
func TestStartNatsCore(t *testing.T){
  nserv := getNatsServerv2()
  state := nserv.StartNatsCore()
  defer nserv.StopNatsCore()
  if !state{
    t.Fatalf("Nats server could not be started with default options")
  }
}

func TestGetConnectionURL (t *testing.T){
  nserv := getNatsServerv2()
  url, err := nserv.GetConnectionURL()
  if url != ""{
    t.Errorf("Connection url made available without server configuation")
  }
  nserv.StartNatsCore()
  defer nserv.StopNatsCore()
  url, err = nserv.GetConnectionURL()
  if err != nil{
    t.Fatalf("Failed to get connection url from the Nats server")
  }
  fmt.Printf("Connection URL for nats server: %s", url)
}

func TestStartNatsCoreSecure(t *testing.T) {
  nserv := getNatsServerv2()
  status, err := nserv.StartNatsCoreSecure()
  if status {
    t.Fatalf("Server should not start without a valid ceritificate for TLS connection")
  }
  nserv.Certfile = "/home/jayaprakash/certs/server/astraserver.pem"
  nserv.Keyfile = "/home/jayaprakash/certs/server/astrakey.pem"
  nserv.CAfile = "/home/jayaprakash/certs/server/ca.pem"
  nserv.VerifyClient = false
  status, err = nserv.StartNatsCoreSecure()
  defer nserv.StopNatsCore()
  if !status {
    t.Fatalf("Server not started with a valid ceritificate: %s", err)
  }
}

func TestGetSecuredConnectionURL(t *testing.T) {
  nserv := getNatsServerv2()
  nserv.Certfile = "/home/jayaprakash/certs/server/astraserver.pem"
  nserv.Keyfile = "/home/jayaprakash/certs/server/astrakey.pem"
  nserv.CAfile = "/home/jayaprakash/certs/server/ca.pem"
  status, err := nserv.StartNatsCoreSecure()
  defer nserv.StopNatsCore()
  if !status {
    t.Fatalf("Server not started with a valid ceritificate: %s", err)
  }
  _, err = nserv.GetSecuredConnectionURL()
  if err != nil {
    t.Fatalf("Failed getting secured url for the server started: %s", err)
  }
}
