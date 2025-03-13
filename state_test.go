package goraft

import (
	"context"
	"fmt"
	"testing"
	"time"
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

func Test3Node(t *testing.T) {
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
	s1.electTimeout = 50 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	go s1.Start(ctx)

	netw2, _ := NewUdpNet(config2)
	s2 := NewState(config2, netw2)
	go s2.Start(ctx)

	netw3, _ := NewUdpNet(config3)
	s3 := NewState(config3, netw3)
	go s3.Start(ctx)

	<-time.After(1 * time.Second)
	cancel()
	if s2.leaderId != 1 || s3.leaderId != 1 {
		t.Fail()
	}
}

func Test5Node(t *testing.T) {
	config := &Config{
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
			{
				Id:   4,
				Addr: "127.0.0.1:18083",
			},
			{
				Id:   5,
				Addr: "127.0.0.1:18084",
			},
		},
	}

	states := genStates(config)
	ctx, cancel := context.WithCancel(context.Background())

	states[2].electTimeout = 50 * time.Millisecond
	for _, s := range states {
		go s.Start(ctx)
	}
	<-time.After(1 * time.Second)
	cancel()
	<-time.After(100 * time.Millisecond)
	if states[3].leaderId != 3 || states[1].leaderId != 3 {
		t.Fail()
	}

}

func TestRepeatLeaderCrash(t *testing.T) {
	for i := 0; i < 10; i++ {
		t.Run(fmt.Sprintf("Run-%d", i), TestLeaderCrash)
	}
}

func TestLeaderCrash(t *testing.T) {
	config := &Config{
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
			{
				Id:   4,
				Addr: "127.0.0.1:18083",
			},
			{
				Id:   5,
				Addr: "127.0.0.1:18084",
			},
		},
	}

	states := genStates(config)
	ctx, cancel := context.WithCancel(context.Background())

	ctx1, cancel1 := context.WithCancel(ctx)
	states[1].electTimeout = 50 * time.Millisecond
	states[2].electTimeout = 100 * time.Millisecond
	for idx, s := range states {
		if idx == 1 {
			go s.Start(ctx1)

		} else {
			go s.Start(ctx)
		}

	}
	<-time.After(2 * time.Second)
	cancel1()
	<-time.After(1 * time.Second)
	cancel()

	leaderId := states[0].leaderId
	for idx, s := range states {
		if idx != 1 && s.leaderId != leaderId {
			t.FailNow()
		}
	}
}

func TestMultiCandidates(t *testing.T) {
	config := &Config{
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
			{
				Id:   4,
				Addr: "127.0.0.1:18083",
			},
			{
				Id:   5,
				Addr: "127.0.0.1:18084",
			},
		},
	}

	states := genStates(config)
	ctx, cancel := context.WithCancel(context.Background())

	states[0].electTimeout = 50 * time.Millisecond
	states[1].electTimeout = 50 * time.Millisecond
	states[2].electTimeout = 50 * time.Millisecond
	for _, s := range states {
		go s.Start(ctx)
	}
	<-time.After(1 * time.Second)
	cancel()
	<-time.After(100 * time.Millisecond)
	if states[3].leaderId > 0 && states[3].leaderId != states[4].leaderId {
		t.Fail()
	}

}

func TestNewFollower(t *testing.T) {
	config := &Config{
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
			{
				Id:   4,
				Addr: "127.0.0.1:18083",
			},
			{
				Id:   5,
				Addr: "127.0.0.1:18084",
			},
		},
	}

	states := genStates(config)
	ctx, cancel := context.WithCancel(context.Background())
	states[1].electTimeout = 50 * time.Millisecond
	for _, s := range states[1:] {
		go s.Start(ctx)
	}
	<-time.After(500 * time.Millisecond)
	go states[0].Start(ctx)
	<-time.After(1 * time.Second)
	cancel()
	<-time.After(1 * time.Second)
	if states[0].leaderId != states[1].leaderId {
		t.Fail()
	}
}

func genStates(config *Config) []*State {
	configs := make([]*Config, 0)
	configs = append(configs, config)
	for i := 1; i < len(config.Nodes); i++ {
		c := &Config{HeartbeatTime: config.HeartbeatTime, AckTimeout: config.AckTimeout}
		c.Nodes = make([]*Node, len(config.Nodes))
		copy(c.Nodes, config.Nodes)
		t := c.Nodes[0]
		c.Nodes[0] = c.Nodes[i]
		c.Nodes[i] = t
		configs = append(configs, c)
	}

	var states []*State
	for _, c := range configs {
		netw, _ := NewUdpNet(c)
		s := NewState(c, netw)
		states = append(states, s)
	}
	return states
}
