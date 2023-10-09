package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

var config configstruct

var dbmap = make(map[string]*sql.DB, 0)
var dbMis *sql.DB

//var dbVimis *sql.DB

var err error

func main() {
	readconfig()

	/*dbmap["db"], err = sql.Open("postgres", config.ConnStr)
	if err != nil {
		//fmt.Println('1')
		fmt.Println(err)
	}
	defer dbmap["db"].Close()*/

	dbmap["dbVimis"], err = sql.Open("postgres", config.ConnStrVimis)
	if err != nil {
		//fmt.Println('1')
		fmt.Println(err)
	}
	defer dbmap["dbVimis"].Close()

	dbMis, err = sql.Open("postgres", `host=10.10.50.3 port=5432 user=postgres password=postgres dbname=postgres sslmode=disable`)
	if err != nil {
		//fmt.Println('1')
		fmt.Println(err)
	}
	defer dbMis.Close()

	//ssh

	//connect to pg via ssh
	/*sshHost := "192.168.88.16"    // SSH Server Hostname/IP
	sshPort := 2225               // SSH Port
	sshUser := "adminbaikal"      // SSH Username
	sshPass := "m7WhM)wWj6v5PK10" // Empty string for no password
	dbUser := "postgres"          // DB username
	dbPass := "postgres"          // DB Password
	dbHost := "localhost"         // DB Hostname/IP
	dbName := "postgres"          // Database name

	var agentClient agent.Agent
	// Establish a connection to the local ssh-agent
	if conn, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK")); err == nil {
		defer conn.Close()

		// Create a new instance of the ssh agent
		agentClient = agent.NewClient(conn)
	}

	// The client configuration with configuration option to use the ssh-agent
	sshConfig := &ssh.ClientConfig{
		User:            sshUser,
		Auth:            []ssh.AuthMethod{},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), //trustedHostKeyCallback(conf.HostKey),
	}

	// When the agentClient connection succeeded, add them as AuthMethod
	if agentClient != nil {
		sshConfig.Auth = append(sshConfig.Auth, ssh.PublicKeysCallback(agentClient.Signers))
	}
	// When there's a non empty password add the password AuthMethod
	if sshPass != "" {
		sshConfig.Auth = append(sshConfig.Auth, ssh.PasswordCallback(func() (string, error) {
			return sshPass, nil
		}))
	}

	// Connect to the SSH Server
	sshcon, err := ssh.Dial("tcp", fmt.Sprintf("%s:%d", sshHost, sshPort), sshConfig)
	if err != nil {
		fmt.Printf("Failed to connect to ssh: %s\n", err.Error())
	}
	defer sshcon.Close()

	// Now we register the ViaSSHDialer with the ssh connection as a parameter
	sql.Register("postgres+ssh", &ViaSSHDialer{sshcon})

	// And now we can use our new driver with the regular postgres connection string tunneled through the SSH connection
	db, err = sql.Open("postgres+ssh", fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", dbUser, dbPass, dbHost, dbName))
	if err != nil {
		fmt.Printf("Failed to connect to the db: %s\n", err.Error())
	}

	defer db.Close()

	fmt.Printf("Successfully connected to the db\n")

	//ssh*/

	ticker := time.NewTicker(time.Minute * 5)
	done := make(chan bool)

	parse()
	go func() {
		for {
			select {
			case <-done:
				return
			case _ = <-ticker.C:
				parse()
			}
		}
	}()
	wait := make(chan string)

	<-wait
	// var input string
	// fmt.Scanln(&input)
	//parse()
}

func parse() {
	parse_radiotherapy()
	parse_pmo()
	parse_newborn()
	parse_death()
	parse_pregnancy()
	parse_polt()
	parse_surgery()
	parse_labtest()
	prikrep()
}
