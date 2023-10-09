package main

import "golang.org/x/crypto/ssh"

type ViaSSHDialer struct {
	client *ssh.Client
}
