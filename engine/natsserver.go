package engine

import (
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"errors"

	ns "github.com/nats-io/nats-server/v2/server"
	jwt "github.com/nats-io/jwt"
)

const username = "astra"
const clusteruser = "astraclusteradmin"

//DefaultOptions - used to start the server with default options
var DefaultOptions = ns.Options{
	Host:               getIPAddress(),
	Port:               4242,
	HTTPPort:           8222,
	ProfPort:           11280,
	NoLog:              true,
	NoSigs:             true,
	MaxConn:            100,
	MaxPayload:         (30 * 1024 * 1024), //for now I have set the default to 30MB- need to look at this later
	MaxControlLine:     1024,
	MaxPending:         1000, //slow consumer threshold
	Username:           username,
	Password:           getdefaultpwd(getIPAddress()),
	AuthTimeout:        1,
	Cluster:						ns.ClusterOpts{
		Host:           getIPAddress(),
		Port:						4244,
		Username:       clusteruser,
		Password:				getclusterdefaultpwd(getIPAddress()),
		AuthTimeout:     0.5,
	},
}


var DefaultSecureOptions = ns.Options{
	Host:               getHostName(),
	Port:               4242,
	HTTPPort:           8222,
	ProfPort:           11280,
	NoLog:              true,
	NoSigs:             true,
	MaxConn:            100,
	MaxPayload:         (30 * 1024 * 1024), //for now I have set the default to 30MB- need to look at this later
	MaxControlLine:     1024,
	MaxPending:         1000, //slow consumer threshold
	AuthTimeout:        1,
	Cluster:						ns.ClusterOpts{
		Host:           getIPAddress(),
		Port:						4244,
		Username:       clusteruser,
		Password:				getclusterdefaultpwd(getIPAddress()),
		AuthTimeout:     0.5,
	},
	tls {
	  cert_file:  "../certs/astraserver.pem",
	  key_file:   "../certs/astrakey.pem",
		ca_file: 		"../certs/ca.pem",
		verify:     false,
	},
}

//NatsCoreServer - Struct used to hold nats configuration
type NatsCoreServer struct {
	Options          *ns.Options
	core             *ns.Server
	MinSubscriptions uint32 //This is newly included to support auto recovery and detect failed subscriptions - JP
	/*For tls support -- All of them has to be defaulted*/
	Certfile				 string
	Keyfile					 string
	CAfile					 string
	VerifyClient		 bool
	/*For trusted Nats enablement -- All of them to be defaulted*/
	EnableTrust			 bool
	Operator 				 string //JWT token file -- optional will be defaulted to
	Resolver				 string //Resolver URL -- will be defaulted to internal server
	urlAccntResolver *ns.URLAccResolver
	operatorClaims   *jwt.OperatorClaims
}

//getHostName - function to get hostname for the machine
func getHostName() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}

	addrs, err := net.LookupIP(hostname)
	if err != nil {
		return hostname
	}

	for _, addr := range addrs {
		if ipv4 := addr.To4(); ipv4 != nil {
			ip, err := ipv4.MarshalText()
			if err != nil {
				return hostname
			}
			hosts, err := net.LookupAddr(string(ip))
			if err != nil || len(hosts) == 0 {
				return hostname
			}
			fqdn := hosts[0]
			return strings.TrimSuffix(fqdn, ".") // return fqdn without trailing dot
		}
	}
	return hostname
}

//getIPAddress - function to get IPaddress for the machine
func getIPAddress() string {
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

//getIP4Address - function to get IPaddress for the machine
func getIP4Address(ethname string) string {
	var ip4add = ""
	interfaces, _ := net.Interfaces()
	for _, inter := range interfaces {
		if addrs, err := inter.Addrs(); err == nil {
			for _, addr := range addrs {
				switch ip := addr.(type) {
				case *net.IPNet:
					if (ip.IP.To4() != nil) && (strings.Compare(inter.Name, ethname) == 0) {
						ip4add = ip.IP.To4().String()
					}
				}
			}
		}
	}
	return ip4add
}

//getdefaultpwd - generates default pwd to be used with nats engine
func getdefaultpwd(hostname string) string {
	ip4bytearry := []byte(hostname)
	prefix := []byte("astrarocks!")
	for idx := range ip4bytearry {
		if ip4bytearry[idx] == byte('.') {
			prefix = append(prefix, byte('_'))
		} else {
			prefix = append(prefix, ip4bytearry[idx])
		}
	}
	return string(prefix)
}

//getclusterdefaultpwd - generates default pwd to be used with nats engine
func getclusterdefaultpwd(hostname string) string {
	ip4bytearry := []byte(hostname)
	prefix := []byte("astracluster!")
	for idx := range ip4bytearry {
		if ip4bytearry[idx] == byte('.') {
			prefix = append(prefix, byte('_'))
		} else {
			prefix = append(prefix, ip4bytearry[idx])
		}
	}
	return string(prefix)
}

//Getoptions - function to get server options
func (server *NatsCoreServer) Getoptions(port int,
	httpport int,
	clusterport int,
	profport int,
	maxconnections int,
	hostname string,
	ethname string) *ns.Options {
	options := &DefaultOptions
	options.Port = port
	options.HTTPPort = httpport
	options.Cluster.Port = clusterport
	options.ProfPort = profport
	options.MaxConn = maxconnections
	if len(hostname) > 0 {
		options.Host = hostname
		options.Cluster.Host = hostname
		options.Password = getdefaultpwd(hostname)
		options.Cluster.Password = getclusterdefaultpwd(hostname)
	}
	if len(ethname) > 0 {
		host := getIP4Address(ethname)
		if len(host) > 0 {
			options.Host = host
			options.Cluster.Host = host
			options.Password = getdefaultpwd(host)
			options.Cluster.Password = getclusterdefaultpwd(host)
		}
	}
	return options
}

//GetNumberOfClient - function to getstatusnotification
func (server *NatsCoreServer) GetNumberOfClient() int {
	return server.core.NumClients()
}

//StartSubscriptionTracking - function to StartSubscriptionTracking
func (server *NatsCoreServer) StartSubscriptionTracking(subscriptiontracker func(holdup bool)) {
	go func() { //Stinky code put in temporarly to survive. Need to have this replaced later
		currentSubscription := server.core.NumSubscriptions()
		hold := false
		for server.IsRunning() {
			if currentSubscription < server.MinSubscriptions {
				if !hold {
					hold = true
					subscriptiontracker(hold)
				}
			} else {
				if hold {
					hold = false
					subscriptiontracker(hold)
				}
			}
			time.Sleep(10 * time.Second) //Should I add this to config?? to be decided later
			currentSubscription = server.core.NumSubscriptions()
		}
	}()
}

//StartNatsCore - function to start the nats core server
func (server *NatsCoreServer) StartNatsCore() bool {
	if server.Options == nil {
		server.Options = &DefaultOptions
	}
	server.core = ns.New(server.Options)
	if server.core == nil {
		panic("Could not start RPC engine")
	}
	go server.core.Start()
	return server.IsRunning()
}

//StartNatsCoreSecure - function to start the nats core server in secure mode
func (server *NatsCoreServer) StartNatsCoreSecure() (bool, error) {
	if server.Options == nil {
		server.Options = &DefaultSecureOptions
	}
	var err error
	err = enableTLS()
	if err != nil {
		return false, err
	}

	if server.EnableTrust {
		err = server.enableTrust()
		if err != nil {
			return false, err
		}
	}

	//Start the nats core server
	server.core = ns.New(server.Options)
	go server.core.Start()
	return server.IsRunning(), nil
}

//enableTLS - function to enableTLS on the server, to be used with StartNatsCoreSecure
func (server *NatsCoreServer) enableTLS() error{
	//Creating TLS Config Options
	tlsconfigoptions := new(ns.TLSConfigOpts)
	tlsconfigoptions.CertFile = server.Certfile
	tlsconfigoptions.KeyFile = server.Keyfile
	if len(server.CAfile) > 0 {
		tlsconfigoptions.CaFile = server.CAfile
	}
	if server.VerifyClient {
		tlsconfigoptions.Verify = true
	} else {
		tlsconfigoptions.Verify = false
	}
	var err error
	server.Options.TLSConfig, err = ns.GenTLSConfig(tlsconfigoptions)
	if err != nil {
		return err
	}
	return nil
}

//enableTrust - function to enable Auth on the server, to be used with StartNatsCoreSecure
func (server *NatsCoreServer) enableTrust() error{
	//Start with Authorization as part of server
	if (len(server.Resolver) > 0) && (len(server.Operator) > 0){
		server.urlAccntResolver, err = ns.NewURLAccResolver(server.Resolver)//set Resolver URL
		if err != nil{
			return err
		}
		//Set Operator JWT
		server.operatorClaims, err = ns.ReadOperatorJWT(server.Operator)
		if err != nil{
			return err
		}
	}
	//Before starting the server ensure account manager is started and url is registered

	return nil
}

//IsRunning - function to check if server is running or not using fake TCP connect
func (server *NatsCoreServer) IsRunning() bool {
	end := time.Now().Add(10 * time.Second)
	for time.Now().Before(end) {
		addr := net.JoinHostPort(server.Options.Host, strconv.Itoa(server.Options.Port))
		//addr := server.core.GetListenEndpoint()
		if addr == "" {
			time.Sleep(10 * time.Millisecond)
			// Retry. We might take a little while to open a connection.
			continue
		}
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			// Retry after 50ms
			time.Sleep(50 * time.Millisecond)
			continue
		}
		conn.Close()
		// Wait a bit to give a chance to the server to remove this
		// "client" from its state, which may otherwise interfere with
		// some tests.
		time.Sleep(25 * time.Millisecond)
		return true
	}
	return false
}

//GetConnectionURL - function to get connection url for the existing server
//sample nats://uid:pwd@127.0.0.1:4242
func (server *NatsCoreServer) GetConnectionURL() (string, error) {
	if server.Options == nil {
		return "", errors.New("Server configuration missing or server is not started")
	}
	serverurl := []byte("nats://")
	uid := []byte(username)
	for idx := range uid {
		serverurl = append(serverurl, uid[idx])
	}
	serverurl = append(serverurl, ':')
	pwd := []byte(getdefaultpwd(server.Options.Host))
	for idx := range pwd {
		serverurl = append(serverurl, pwd[idx])
	}
	serverurl = append(serverurl, '@')
	ipaddr4 := []byte(server.Options.Host)
	for idx := range ipaddr4 {
		serverurl = append(serverurl, ipaddr4[idx])
	}
	serverurl = append(serverurl, ':')
	port := []byte(strconv.Itoa(server.Options.Port))
	for idx := range port {
		serverurl = append(serverurl, port[idx])
	}
	return string(serverurl), nil
}

//GetConnectionURL - function to get connection url for the existing server
//sample tls://localhost:4242
func (server *NatsCoreServer) GetSecuredConnectionURL() (string, error) {
	if server.Options == nil {
		return "", errors.New("Server configuration missing or server is not started")
	}
	serverurl := []byte("tls://")
	ipaddr4 := []byte(server.Options.Host)
	for idx := range ipaddr4 {
		serverurl = append(serverurl, ipaddr4[idx])
	}
	serverurl = append(serverurl, ':')
	port := []byte(strconv.Itoa(server.Options.Port))
	for idx := range port {
		serverurl = append(serverurl, port[idx])
	}
	return string(serverurl), nil
}

//StopNatsCore - function to stop the nats core server
func (server *NatsCoreServer) StopNatsCore() {
	if server.core != nil{
		server.core.Shutdown()
	}
}
