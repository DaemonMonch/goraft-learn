package goraft

import (
	"context"
	"testing"
)

func TestOneNode(t *testing.T) {
	config1 := &Config{
		HeartbeatTime: 10,
		AckTimeout:    20,
		Nodes: []*Node{
			&Node{
				Id:   1,
				Addr: "127.0.0.1:18080",
			},
		},
	}

	netw, _ := NewUdpNet(config1)
	s := NewState(config1, netw)
	s.Start(context.Background())

}

func TestMultiNode(t *testing.T) {
	config1 := &Config{
		HeartbeatTime: 10,
		AckTimeout:    20,
		Nodes: []*Node{
			{
				Id:   1,
				Addr: "127.0.0.1:18080",
			},
			{
				Id:   2,
				Addr: "127.0.0.1:18081",
			},
			{
				Id:   3,
				Addr: "127.0.0.1:18082",
			},
		},
	}
	config2 := &Config{
		HeartbeatTime: 10,
		AckTimeout:    20,
		Nodes: []*Node{
			{
				Id:   2,
				Addr: "127.0.0.1:18081",
			},
			{
				Id:   1,
				Addr: "127.0.0.1:18080",
			},

			{
				Id:   3,
				Addr: "127.0.0.1:18082",
			},
		},
	}
	config3 := &Config{
		HeartbeatTime: 10,
		AckTimeout:    20,
		Nodes: []*Node{
			{
				Id:   3,
				Addr: "127.0.0.1:18082",
			},
			{
				Id:   1,
				Addr: "127.0.0.1:18080",
			},
			{
				Id:   2,
				Addr: "127.0.0.1:18081",
			},
		},
	}

	netw1, _ := NewUdpNet(config1)
	s1 := NewState(config1, netw1)
	go s1.Start(context.Background())

	netw2, _ := NewUdpNet(config2)
	s2 := NewState(config2, netw2)
	go s2.Start(context.Background())

	netw3, _ := NewUdpNet(config3)
	s3 := NewState(config3, netw3)
	s3.Start(context.Background())

}
