package goraft

import (
	"encoding/json"
	"net"
	"net/netip"
	"os"
)

type Networking interface {
	Send(node *Node, msg RaftMessage) error
	Recv() (<-chan RaftMessage, error)
	Broadcast(voteReq RaftMessage) error
}

type nodeConn struct {
	conn *net.UDPConn
	node *Node
}

type UdpNet struct {
	l      *net.UDPConn
	config *Config

	nodeConns map[int]*nodeConn
	curNode   *Node
}

func NewUdpNet(config *Config) (*UdpNet, error) {
	l, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort(config.Nodes[0].Addr)))
	if err != nil {
		return nil, err
	}
	udpNet := &UdpNet{l: l, config: config, nodeConns: make(map[int]*nodeConn), curNode: config.Nodes[0]}
	for _, node := range config.Nodes[1:] {
		conn, err := net.DialUDP("udp", nil, net.UDPAddrFromAddrPort(netip.MustParseAddrPort(node.Addr)))
		if err != nil {
			return nil, err
		}
		n := node
		nc := &nodeConn{
			conn: conn,
			node: n,
		}
		udpNet.nodeConns[n.Id] = nc
	}

	return udpNet, nil
}

func (n *UdpNet) Broadcast(msg RaftMessage) error {
	for _, nc := range n.nodeConns {
		if err := n.Send(nc.node, msg); err != nil {
			logger.Printf("send to [%v] msg [%v] fail, err [%v]", nc.node, msg, err)
		}
	}
	return nil
}

func (n *UdpNet) Send(node *Node, msg RaftMessage) error {
	to := n.nodeConns[node.Id]
	msg.NodeId = n.curNode.Id
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = to.conn.Write(b)
	if err != nil {
		return err
	}

	return nil
}

func (n *UdpNet) Recv() (<-chan RaftMessage, error) {
	msgChan := make(chan RaftMessage, 1)
	decoder := json.NewDecoder(n.l)
	go func() {

		for {
			var msg RaftMessage
			err := decoder.Decode(&msg)
			if err != nil && err != os.ErrDeadlineExceeded {
				logger.Printf("recv msg fail err [%v]\n", err)
				break
			}
			msg.from = n.nodeConns[msg.NodeId].node
			msgChan <- msg
		}
		close(msgChan)

	}()

	return msgChan, nil
}

func (n *UdpNet) Close() {
	n.l.Close()

}
