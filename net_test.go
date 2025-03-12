package goraft

import "testing"

func TestNet(t *testing.T) {
	config1 := &Config{
		HeartbeatTime: 10,
		AckTimeout:    20,
		Nodes: []*Node{
			&Node{
				Id:   1,
				Addr: "127.0.0.1:18080",
			},
			&Node{
				Id:   2,
				Addr: "127.0.0.1:18081",
			},
		},
	}

	config2 := &Config{
		HeartbeatTime: 10,
		AckTimeout:    20,
		Nodes: []*Node{
			&Node{
				Id:   2,
				Addr: "127.0.0.1:18081",
			},
			&Node{
				Id:   1,
				Addr: "127.0.0.1:18080",
			},
		},
	}

	node1, err := NewUdpNet(config1)
	if err != nil {
		t.Fatal(err)
	}
	node2, err := NewUdpNet(config2)
	if err != nil {
		t.Fatal(err)
	}
	node1.Send(config1.Nodes[1], NewAckMsg(1))

	c, err := node2.Recv()

	if err != nil {
		t.Fatal(err)
	}
	msg := <-c
	t.Logf("msg [%v]\n", msg)
	if msg.Typo != ACK_MESSAGE_TYPE {
		t.Fail()
	}

	node2.Close()
	node1.Close()
}
