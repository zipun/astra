package engine

import (
  "os"
  "net"
)

func getAccountServer() *AccountServer{
  accserv := new(AccountServer)
  accserv.Hostport = getIPAddressAccServer() + ":0"
  accserv.NSCFolder = "/home/jayaprakash/astra_v2/github.com/jayaprakash-j/astra/nscfolder"
  accserv.NATSUrl = "nats://astra:astrarocks!10_0_2_15@10.0.2.15:4242"
  return accserv
}

//getIPAddress - function to get IPaddress for the machine
func getIPAddressAccServer() string {
	var ip4add = "localhost"
	host, err := os.Hostname()
	if err != nil {
		panic("Cannot retrieve default hostname of the server")
	}
	iparry, err := net.LookupIP(host)
	if err != nil {
		panic("Cannot retrieve IPAddr from the server. Please use config file for providing the values.")
	}
	for _, ip := range iparry {
		if ip4 := ip.To4(); ip4 != nil {
			ip4add = ip.String()
		}
	}
	return ip4add
}
