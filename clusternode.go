package main

import (
	"net"
	"strconv"

	"github.com/Sirupsen/logrus"
	"github.com/gosexy/redis"
)

type ClusterNode struct {
	r        *redis.Client
	flags    []string
	port     uint
	host     string
	replicas bool
	dirty    bool
	friends  []string
}

func NewClusterNode(addr string) (node *ClusterNode) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		logrus.Error(err)
		return nil
	}

	p, _ := strconv.ParseUint(port, 10, 0)
	node = &ClusterNode{
		host:     host,
		port:     uint(p),
		replicas: false,
		dirty:    false,
	}

	return node
}

func (self *ClusterNode) Connect() (err error) {
	var client *redis.Client

	if self.r != nil {
		return nil
	}

	client = redis.New()
	if err = client.Connect(self.host, self.port); err != nil {
		return err
	}

	if _, err = client.Ping(); err != nil {
		return err
	}

	self.r = client
	return nil
}

func (self *ClusterNode) Friends() []string {
	return self.friends
}

func (self *ClusterNode) Slots() []string {
	return nil
}
