package engine

import (
  nas "github.com/nats-io/nats-account-server/server/core"
)

//NatsAccountServer - used to manage JWT based authentication for NATS that will become basis for ASTRA
type AccountServer struct {
  accserv   *nas.AccountServer
  flags     nas.Flags
  Hostport  string
  NSCFolder string
  NATSUrl   string
  Debug     bool
  Verbose   bool
}

//Initialize - func used to initialize the account server
func (a *AccountServer) Initialize() error {
  a.accserv = nas.NewAccountServer()
  a.flags = nas.Flags{}
  a.flags.NSCFolder = a.NSCFolder
  a.flags.ReadOnly = true
  a.flags.NATSURL = a.NATSUrl
  a.flags.HostPort = a.Hostport
  a.flags.Debug = a.Debug
  a.flags.Verbose = a.Verbose
  err := a.accserv.InitializeFromFlags(a.flags)
  return err
}

//Start - func used to start the account server
func (a *AccountServer) Start() error {
  if a.accserv != nil {
    return a.accserv.Start()
  }
  return nil
}

//Stop - func used to stop the account server
func (a *AccountServer) Stop() {
  if a.accserv != nil {
    a.accserv.Stop()
  }
}
