package goraft

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"time"
)

const (
	FOLLOWER_STATE = 1 << iota
	CANDIDATE_STATE
	LEADER_STATE
)

const (
	REQ_VOTE_MESSAGE_TYPE = iota
	VOTE_RESPOND_MESSAGE_TYPE
	HEARTBEAT_MESSAGE_TYPE
	ACK_MESSAGE_TYPE
)

var logger = log.New(os.Stdout, "raft ", log.Lmsgprefix|log.Ldate|log.Ltime)

type Config struct {
	Nodes         []*Node `json:"nodes"`
	HeartbeatTime int     `json:"heartbeatTime"`
	AckTimeout    int     `json:"ackTimeout"`
}

type Node struct {
	Id   int    `json:"id"`
	Addr string `json:"addr"`
}

func (n *Node) String() string {
	return fmt.Sprintf("Node{id=%d,addr=%s}", n.Id, n.Addr)
}

type State struct {
	state        int
	epoch        int
	electTimeout time.Duration
	config       *Config

	networking Networking
	votes      int
	curNode    *Node
	leaderId   int

	//LEADER
	lastHbTime  time.Time
	hbCloseChan chan struct{}
	acks        map[int]time.Time

	//CANDIDATE
	voteId int
}

func NewState(config *Config, networking Networking) *State {
	s := &State{
		state:        FOLLOWER_STATE,
		config:       config,
		electTimeout: time.Duration(rand.Intn(150)+150) * time.Millisecond,
		networking:   networking,
		curNode:      config.Nodes[0],
	}
	return s
}

func (s *State) Start(ctx context.Context) error {
	timer := time.NewTicker(s.electTimeout)
	defer timer.Stop()
	msgChan, err := s.networking.Recv()
	if err != nil {
		return err
	}
	for {
		select {
		case t := <-timer.C:
			s.onElectTimeout(t)
		case msg := <-msgChan:
			s.OnMessage(msg)
		case <-ctx.Done():
			return nil
		}

	}
}

func (s *State) onElectTimeout(ticker time.Time) {
	if ticker.Sub(s.lastHbTime) <= s.electTimeout {
		return
	}
	if s.state == FOLLOWER_STATE {
		logger.Printf("node [%s] state [%d] exceed elect timeout  transit to state [CANDIDATE] \n", s.curNode.String(), s.state)
		s.state = CANDIDATE_STATE
		s.epoch += 1
		voteReq := NewVoteMsg(s.epoch)
		s.networking.Broadcast(voteReq)

	} else if s.state == CANDIDATE_STATE {
		logger.Printf("node [%s] state [%d] exceed elect timeout  , start new elect \n", s.curNode.String(), s.state)
		s.epoch += 1
		voteReq := NewVoteMsg(s.epoch)
		s.networking.Broadcast(voteReq)
	}
}

func (s *State) OnMessage(msg RaftMessage) {
	t := msg.Typo
	switch t {
	case REQ_VOTE_MESSAGE_TYPE:
		s.onVoteMsg(msg)
	case VOTE_RESPOND_MESSAGE_TYPE:
		s.onVoteRespMsg(msg)
	case ACK_MESSAGE_TYPE:
		s.onAckMsg(msg)
	default:
		s.onHbMsg(msg)
	}
}

func (s *State) onAckMsg(msg RaftMessage) {
	now := time.Now()
	s.acks[msg.from.Id] = now
	for k := range s.acks {
		if now.Sub(s.acks[k]) > time.Duration(s.config.AckTimeout)*time.Millisecond {
			logger.Printf("node [%s] state [LEADER] node [%d] ack exceed timeout [%d]ms \n", s.curNode.String(), k, s.config.AckTimeout)
		}
	}
}

func (s *State) onVoteRespMsg(msg RaftMessage) {
	logger.Printf("node [%s] state [%d] vote resp reveived\n", s.curNode.String(), s.state)

	if s.state == CANDIDATE_STATE && msg.VoteNodeId == s.curNode.Id {
		s.votes += 1
		if s.votes >= int(math.Ceil(float64(len(s.config.Nodes))/2)) {
			logger.Printf("node [%s] state [%d] reveive majority vote [%d] from all nodes, transit to state [LEADER]\n", s.curNode.String(), s.state, s.votes)
			s.state = LEADER_STATE
			s.voteId = 0
			s.acks = make(map[int]time.Time)
			s.startHeartbeat()
		}
	}
}

func (s *State) startHeartbeat() {
	go func() {
		t := time.NewTicker(time.Millisecond * time.Duration(s.config.HeartbeatTime))
		defer t.Stop()
		for {
			select {
			case <-s.hbCloseChan:
				return
			case <-t.C:
				s.networking.Broadcast(NewHb(s.epoch))
			}
		}
	}()
}

func (s *State) onHbMsg(msg RaftMessage) {
	// logger.Printf("node [%s] state [%d] recv hb msg from node [%d], leader id [%d]\n", s.curNode.String(), s.state, msg.NodeId, s.leaderId)
	s.lastHbTime = time.Now()
	if s.state == FOLLOWER_STATE || s.state == CANDIDATE_STATE {
		logger.Printf("node [%s] state [%d] recv hb msg, transit to [FOLLOWER], leader id [%d]\n", s.curNode.String(), s.state, msg.NodeId)
		s.state = FOLLOWER_STATE
		s.votes = 0
		s.voteId = 0
		s.epoch = msg.Epoch
		s.leaderId = msg.NodeId
	}

	ack := NewAckMsg(s.epoch)
	s.networking.Send(msg.from, ack)
}

func (s *State) onVoteMsg(msg RaftMessage) {
	logger.Printf("node [%s] state [%d] recv vote req from node  [%d]\n", s.curNode.String(), s.state, msg.from.Id)
	resp := NewVoteResp(s.epoch)
	if s.voteId > 0 {
		logger.Printf("node [%s] state [%d] already vote for node [%d], disagree vote req\n", s.curNode.String(), s.state, s.voteId)
		return
	}
	if s.epoch >= msg.Epoch {
		logger.Printf("node [%s] state [%d] local epoch [%d] >= vote req epoch [%d], disagree vote req\n", s.curNode.String(), s.state, s.epoch, msg.Epoch)
		resp.VoteNodeId = s.curNode.Id
	} else {
		logger.Printf("node [%s] state [%d] local epoch [%d] < vote req epoch [%d], agree vote req\n", s.curNode.String(), s.state, s.epoch, msg.Epoch)
		resp.VoteNodeId = msg.NodeId
	}
	s.voteId = resp.VoteNodeId
	//todo : other check
	s.networking.Send(msg.from, resp)
}

type RaftMessage struct {
	from       *Node
	NodeId     int `json:"nodeId"`
	Typo       int `json:"typo"`
	Epoch      int `json:"epoch"`
	VoteNodeId int `json:"voteNodeId"`
}

func NewVoteMsg(epoch int) RaftMessage {
	return RaftMessage{Typo: REQ_VOTE_MESSAGE_TYPE, Epoch: epoch}
}

func NewAckMsg(epoch int) RaftMessage {
	return RaftMessage{Typo: ACK_MESSAGE_TYPE, Epoch: epoch}
}

func NewHb(epoch int) RaftMessage {
	return RaftMessage{Typo: HEARTBEAT_MESSAGE_TYPE, Epoch: epoch}
}

func NewVoteResp(epoch int) RaftMessage {
	return RaftMessage{Typo: VOTE_RESPOND_MESSAGE_TYPE, Epoch: epoch}
}
