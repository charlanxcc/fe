/* fast election */

// go build -o fe fe.go

/*
  init-------(sync req)---->syncing
     +-------(put req)----->proposing
     +-------(proposal)---->holding
     +-------(commit)------>init
     \-------(others)------>init

  syncing----(got quorum)-->init
        \----(no quorum)--->syncing

  proposing--(got quorum)------>commiting
          +--(we candidate)---->proposing
          +--(other candiate)-->yielding
          +--(yiled req)------->yielding
          +--(commit req)------>proposing
          +--(no quorum)------->failure
          \--(others)---------->proposing

  holding--(yield)----->holding
        +--(commit)---->init
        \--(timeout)--->init

  commiting--(regardless)-->init

  yielding---(regardless)-->proposing

  failure-->init

 */

package fe

import (
	_ "bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	_ "reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var peerConnectionError error = fmt.Errorf("peer connection protocol error")

type Peer struct {
	Id			string		`json:"id"`
	Host		string		`json:"host"`
	Port		int			`json:"port"`

	connOut		net.Conn	`json:"-"`	// outgoing
	connIn		net.Conn	`json:"-"`	// incoming

	Self		bool		`json:"self"`
	State		bool		`json:"status"`	// up or down

	// stats
}

type Client struct {
	Id			string		`json:"id"`
	conn		net.Conn	`json:"-"`

	// stats
}

/* methods
1. connect
2. client
3. ping / pong
4. result
5. get
6. info
7. hold - holding
8. yield - holding
9. cancel
10. commit

11. status
12. error

*/

/* states:
 init
 failure
 success
 proposing
 commiting
 yielding
 holding
 */

/*
type: connect
  -- required: Key, Value
  Key: "Id"
  Value: id of the peer

type: ack
  -- required: none

type: ping
  -- required: none

type: pong
  -- required: none

type: data
  -- required: all fields
  -- it can be committed or proposed data

type: error
  -- required: Channel, State, Phase, Key & Value
  key: error code
  value: error message

type: hold
  -- required:  all fields

type: holding
  -- required: Channel, Phase, Index

type: commit
  -- required: Channel, Index

type: commit-full
  -- required: all fields

type: committed
  -- required: Channel, Phase, Index

type: cancel
  -- required: Channel, Index

type: canceled
  -- required: Channel, Phase, Index

type: yield
  -- required: Channel, Issuer, Issuer

type: yielded
  -- required: Channel, Phase, Index

type: get - returns the current value (if State == committed) or
    proposal if State == proposed
  -- required: Channel, State

type: query
  -- required: Channel, Key

type: put
  -- required: Channel, Key & Value

type: sync
  -- required: Channel

type: status
  -- required: none

type: timeout
  -- required: Channel, Phase


-- tracker status
  queued
  starting
  proposing
  yielded
  committing
  committed
  canceled
  done
*/

type FeData struct {
	Type		string		`json:"type"`
	From		string		`json:"from"`
	Channel		string		`json:"channel"`
	State		string		`json:"state"`
	Phase		uint64		`json:"phase"`
	Index		uint64		`json:"index"`
	Issuer		string		`json:"issuer"`
	Key			string		`json:"key"`
	Value		string		`json:"value"`
	Timestamp	int64		`json:"timestamp"`
	HoldUntil	int64		`json:"hold-until"`

	next		*FeData		`json:"-"`
	tracker		func(*FeData, string, error)	`json:"-"`
}

var (
	connsLock = sync.RWMutex{}
	peers []*Peer
	peersById = map[string]*Peer{}
	peersByConn = map[net.Conn]*Peer{}

	clientsByConn = map[net.Conn]*Client{}

	me *Peer
	listenConn net.Conn

	feDataGenesis = &FeData{
		Type: "data",
		From: "genesis",
		Channel: "genesis",
		State: "committed",
		Phase: 1,
		Index: 1,
		Issuer: "genesis",
		Key: "genesis",
		Value: "genesis",
		Timestamp: 1,
		HoldUntil: 0,
	}

	feErrorWrongIndex = &FeData{
		Type: "error",
		Key: "101",
		Value: "Wrong index",
	}

	feErrorSendFailure = &FeData{
		Type: "error",
		Key: "100",
		Value: "Failed to send",
	}

	defch = NewFeCollator("default-channel")
)

func (d *FeData) Id() string {
	return fmt.Sprintf("%s.%d.%d", d.Issuer, d.Index, d.Timestamp)
}

func (d *FeData) Cmp(e *FeData, checkValue bool) int {
	if v := strings.Compare(d.Issuer, e.Issuer); v != 0 {
		return v
	} else if v := d.Index - e.Index; v != 0 {
		return int(v)
	} else if v := d.Timestamp - e.Timestamp; v != 0 {
		return int(v)
	} else if !checkValue {
		return 0
	} else if v := strings.Compare(d.Key, e.Key); v != 0 {
		return v
	} else if v := strings.Compare(d.Value, e.Value); v != 0 {
		return v
	} else if v := strings.Compare(d.State, e.State); v != 0 {
		return v
	} else {
		return 0
	}
}

func sendBytes(conn net.Conn, data []byte) error {
	l := len(data)

	err := binary.Write(conn, binary.LittleEndian, int32(l))
	if err != nil {
		return err
	}
	_, err = conn.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func sendString(conn net.Conn, data string) error {
	return sendBytes(conn, []byte(data))
}

func sendJson(conn net.Conn, q interface{}) error {
	qs, err := json.Marshal(q)
	if err != nil {
		return err
	}
	return sendBytes(conn, qs)
}

func recvBytes(conn net.Conn) ([]byte, error) {
	var l int32
	err := binary.Read(conn, binary.LittleEndian, &l)
	if err != nil {
		return nil, err
	}
	data := make([]byte, l)
	n, err := conn.Read(data)
	if err != nil || n != int(l) {
		if err == nil {
			err = fmt.Errorf("incomplete data")
		}
		return nil, err
	}
	return data, nil
}

func recvString(conn net.Conn) (string, error) {
	data, err := recvBytes(conn)
	if err != nil {
		return "", err
	} else {
		return string(data), nil
	}
}

func recvJson(conn net.Conn, v interface{}) error {
	q, err := recvBytes(conn)
	if err != nil {
		return err
	}
	err = json.Unmarshal(q, v)
	if err != nil {
		return err
	}
	return nil
}

func (p *Peer) connect(peer *Peer) {
	for {
		conn, err := net.Dial("tcp",
			net.JoinHostPort(peer.Host, strconv.Itoa(peer.Port)))
		if err != nil {
			//fmt.Printf("failed to connect to %s:%d: %s\n", peer.Host, peer.Port, err)
			time.Sleep(5 * time.Second)
			continue
		}

		/* send connection request */
		data := FeData{
			Type: "connect",
			Key: "id",
			Value: p.Id,
		}
		err = sendJson(conn, data)
		if err != nil {
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		/* receive the result */
		err = recvJson(conn, &data)
		if err != nil {
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		if data.Type != "ack" {
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		connsLock.Lock()
		peer.connOut = conn
		peersByConn[conn] = peer
		peer.State = true
		connsLock.Unlock()

		//fmt.Printf("XXX: connection established to %s\n", peer.Id)
		p.peerHandler(peer, conn)

		connsLock.Lock()
		delete(peersByConn, conn)
		peer.connOut = nil
		if peer.connIn == nil {
			peer.State = false
		}
		connsLock.Unlock()
	}

	return
}

func (p *Peer) getConn() (net.Conn, error) {
	if p.connOut != nil {
		return p.connOut, nil
	} else if p.connIn != nil {
		return p.connIn, nil
	} else {
		return nil, syscall.ENOTCONN
	}
}

func (p *Peer) sendBytes(data []byte) error {
	conn, err := p.getConn()
	if err != nil {
		return err
	}
	return sendBytes(conn, data)
}

func (p *Peer) sendString(data string) error {
	conn, err := p.getConn()
	if err != nil {
		return err
	}
	return sendString(conn, data)
}

func (p *Peer) sendJson(data interface{}) error {
	conn, err := p.getConn()
	if err != nil {
		return err
	}
	return sendJson(conn, data)
}

func (p *Peer) peerHandler(peer *Peer, conn net.Conn) {
	var req FeData
	var err error

	/* p is me, peer is the peer */
	for {
		err = recvJson(conn, &req)
		if err != nil {
			if err == io.EOF {
				//log.Printf("Connection from %s closed.\n", peer.Id)
			} else {
				log.Printf("failed to receive data from peer %s: %s\n",
					peer.Id, err)
			}
			break
		}

		req.From = peer.Id
		err = FeventProcessData(&req)
	}
}

func (p *Peer) clientHandler(client *Client, conn net.Conn) {
	var req, rsp FeData
	var err error

	/* p is me, client is the client */
	for {
		err = recvJson(conn, &req)
		if err != nil {
			if err == io.EOF {
				//log.Printf("Connection from %s closed.\n", client.Id)
			} else {
				log.Printf("failed to receive data from peer %s: %s\n",
					client.Id, err)
			}
			break
		}

		switch req.Type {
		case "sync":
			req.From = me.Id
			req.tracker = func(data *FeData, state string, err error) {
				rsp = req
				rsp.State = state
				switch state {
				case "done": fallthrough
				case "canceled":
					e2 := sendJson(conn, &rsp)
					if e2 != nil {
						fmt.Printf("failed to send to %s: %s\n", client.Id, e2)
					}
				}
			}
			defch.putTask(&req)

		case "get":
			err = sendJson(conn, defch.data)
			if err != nil {
				fmt.Printf("failed to send to %s: %s\n", client.Id, err)
				return
			}

		case "put":
			req.From = me.Id
			req.tracker = func(data *FeData, state string, err error) {
				rsp = req
				rsp.State = state
				switch state {
				case "committed": fallthrough
				case "canceled":
					e2 := sendJson(conn, &data)
					if e2 != nil {
						fmt.Printf("failed to send to %s: %s\n", client.Id, e2)
					}
				}
			}
			defch.putTask(&req)

		case "status":
			rsp.Type = "data"
			rsp.Value, _ = status()
			err = sendJson(conn, rsp)
			if err != nil {
				fmt.Printf("failed to send to %s: %s\n", client.Id, err)
				return
			}
		default:
			rsp.Type = "error"
			rsp.Key = "1"
			rsp.Value = "Unknown request type"
			err = sendJson(conn, rsp)
			if err != nil {
				fmt.Printf("failed to send to %s: %s\n", client.Id, err)
				return
			}
		}
	}
}

func (p *Peer) accept(conn net.Conn) {
	defer conn.Close()

	var req *FeData
	err := recvJson(conn, &req)
	if err != nil {
		log.Printf("did not receive proper greeting: %s\n", err)
		return
	}

	if !(req.Type == "connect" || req.Type == "client") || req.Value == "" {
		log.Printf("did not receive proper greeting.\n")
		return
	}

	if req.Type == "client" {
		rsp := &FeData{
			Type: "ack",
			Key: "id",
			Value: "ok",
		}

		clientId := req.Value
		client := &Client{Id: clientId, conn: conn}

		err = sendJson(conn, rsp)
		if err != nil {
			log.Printf("failed to return greeting: %s\n", err)
			return
		}

		connsLock.Lock()
		clientsByConn[conn] = client
		connsLock.Unlock()

		//fmt.Printf("XXX: connection established from client %s\n", client.Id)
		p.clientHandler(client, conn)

		connsLock.Lock()
		delete(clientsByConn, conn)
		client.conn = nil
		connsLock.Unlock()
	} else /* if req.Method == "connect" */ {
		rsp := &FeData{
			Type: "ack",
			Key: "id",
			Value: "ok",
		}

		peerId := req.Value
		connsLock.RLock()
		peer, ok := peersById[peerId]
		connsLock.RUnlock()
		if !ok {
			rsp.Type = "error"
			rsp.Key = "1"
			rsp.Value = "unknown id"
			log.Printf("greetings from unknown id %s.\n", peerId)
			err = sendJson(conn, rsp)
			if err != nil {
				log.Printf("failed to return greeting: %s\n", err)
			}
			return
		}

		err = sendJson(conn, rsp)
		if err != nil {
			log.Printf("failed to return greeting: %s\n", err)
			return
		}

		connsLock.Lock()
		peer.connIn = conn
		peersByConn[conn] = peer
		peer.State = true
		connsLock.Unlock()

		//fmt.Printf("XXX: connection established from %s\n", peer.Id)
		p.peerHandler(peer, conn)

		connsLock.Lock()
		delete(peersByConn, conn)
		peer.connIn = nil
		if peer.connOut == nil {
			peer.State = false
		}
		connsLock.Unlock()
	}
	return
}

func (p *Peer) listen() {
	listenConn, err := net.Listen("tcp", fmt.Sprintf(":%d", p.Port))
	if err != nil {
		log.Fatal("cannot listen on:", p.Port, ",", err)
		return
	}
	//fmt.Printf("listening on %d\n", p.Port)

	for {
		conn, err := listenConn.Accept()
		if err != nil {
			log.Printf("Accept failed:", err)
			// TODO: exit if fatal
			continue
		}

		go p.accept(conn)
	}
}

func status() (string, error) {
	q := &bytes.Buffer{}

	q.WriteString(`{"peers":`)
	b, _ := json.Marshal(peers)
	q.Write(b)
	q.WriteString(`, "clients": [`)
	connsLock.RLock()
	first := true
	for _, x := range clientsByConn {
		if first {
			first = false
		} else {
			q.WriteString(",")
		}
		b, _ := json.Marshal(x)
		q.Write(b)
	}
	connsLock.RUnlock()
	q.WriteString("]")

	q.WriteString(`, "status": `)
	s, e := json.Marshal(defch)
	if e != nil {
		q.WriteString(fmt.Sprintf(`{"error": "%s",}`, e))
	} else {
		q.WriteString(string(s))
	}
	
	q.WriteString("}")
	return q.String(), nil
}

/*
func test_peer_handler(conn net.Conn) {
	defer conn.Close()

	peerId, err := acceptPeer(conn)
	if err != nil {
		fmt.Printf("failed to accept peer: %s\n", err)
		return
	}

	fmt.Printf("Got peer id '%s'\n", peerId)

	for {
		data, err := recvString(conn)
		if err != nil {
			if err != io.EOF {
			}
			break
		}

		r := []rune(data)
		for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
			r[i], r[j] = r[j], r[i]
		}
		err = sendString(conn, string(r))
		if err != nil {
			break
		}
	}
}

func test_listener(port int) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		fmt.Errorf("Cannot listen on %d: %s", port, err)
		return
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal("Accept failed:", err)
			break
		}

		go test_peer_handler(conn)
	}
}

func test_client(host string, port int, auto bool) {
	conn, err := connectToPeer(host, port, "who am i")
	if err != nil {
		log.Fatal("Cannot connect:", err)
		return
	}

	defer conn.Close()

	ix := 0
	in := bufio.NewReader(os.Stdin)
	for {
		var line string
		var err error

		if !auto {
			line, err = in.ReadString('\n')
			if err != nil {
				line = strings.TrimRight(line, "\n")
			}
		} else {
			line = fmt.Sprintf("xxx-%d\n", ix)
			ix = ix + 1
		}

		err = sendString(conn, line)
		if err != nil {
			fmt.Printf("failed to send: %s\n", err)
			break
		}

		line, err = recvString(conn)
		if err != nil {
			fmt.Printf("failed to recv: %s\n", err)
			break
		}

		fmt.Printf("%s\n", line)
	}
}
*/

/* fast election code */

func loadConfig(fn string) error {
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &peers)
	if err != nil {
		return err
	}
	return nil
}

func sendTo(id string, data *FeData) error {
	connsLock.RLock()
	p, ok := peersById[id]
	if !ok {
		connsLock.RUnlock()
		return fmt.Errorf("Unknown id %s", id)
	}
	if p.Self {
		connsLock.RUnlock()
		return fmt.Errorf("%s is self", id)
	}
	c, err := p.getConn()
	connsLock.RUnlock()
	if err != nil {
		return err
	}
	return sendJson(c, data)
}

func sendToPeers2(peers []string, data *FeData) {
	for _, i := range peers {
		_ = sendTo(i, data)
	}
}

func sendToPeers(peers []*Peer, data *FeData) {
	for _, i := range peers {
		if i.Self || !i.State {
			continue
		}
		go func(i *Peer) {
			err := i.sendJson(data)
			if err != nil {
				fmt.Printf("failed to send: %s\n", err)
			}
		}(i)
	}
}

func run(conf, id string) {
	err := loadConfig(conf)
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, i := range peers {
		if i.Id == id {
			i.Self = true
			i.State = true
		}
		peersById[i.Id] = i
	}

	if x, ok := peersById[id]; ok {
		//fmt.Printf("me is %s\n", x.Id)
		me = x
	} else {
		fmt.Printf("unknown id %s\n", id)
		return
	}

	defch.setPeers(me, peers)

	for _, i := range peers {
		if me == i {
			continue
		}
		go me.connect(i)
	}

	me.listen()
	return
}

func testListen(port string) {
	lconn, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatal("cannot listen on:", port, ",", err)
		return
	}

	go func(c net.Listener) {
		d, e := c.Accept()
		if e != nil {
			d.Close()
		}
	}(lconn)

	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%s", port))
	if err != nil {
		log.Fatal("cannot connect to:", port, ",", err)
		return
	}

	fmt.Printf("press any key (with \\n): ")
	var b []byte = make([]byte, 1)
	os.Stdin.Read(b)

	conn.Close()
}

func usage() {
	fmt.Printf(`Usage: %s [run <config.json> <id> | test-listen <port> |
	status <host>:<port> | sync <host>:<port> | get <host>:<port> |
	query <host>:<port> <key> | put <host>:<port> <key> <value> |
	bulk-put <host>:<port> prefix start end]
`,
		os.Args[0])
}

func FeMain() {
	if len(os.Args) <= 1 {
		usage()
		os.Exit(1)
/*
	} else if os.Args[1] == "server" && len(os.Args) == 3 {
		port, err := strconv.Atoi(os.Args[2])
		if err != nil || port <= 0 || port > 65535 {
			usage()
			os.Exit(1)
		}
		test_listener(port)
	} else if os.Args[1] == "client" && len(os.Args) == 3 {
		ls := strings.Split(os.Args[2], ":")
		if len(ls) != 2 {
			usage()
			os.Exit(1)
		}
		port, err := strconv.Atoi(ls[1])
		if err != nil || port <= 0 || port > 65535 {
			usage()
			os.Exit(1)
		}
		test_client(ls[0], port, false)
*/
	} else if os.Args[1] == "run" && len(os.Args) == 4 {
		run(os.Args[2], os.Args[3])
	} else if os.Args[1] == "test-listen" && len(os.Args) == 3 {
		testListen(os.Args[2])
	} else if (os.Args[1] == "status" && len(os.Args) == 3) ||
		(os.Args[1] == "sync" && len(os.Args) == 3) ||
		(os.Args[1] == "get" && len(os.Args) == 3) ||
		(os.Args[1] == "put" && len(os.Args) == 5) ||
		(os.Args[1] == "bulk-put" && len(os.Args) == 6) {
		FeClient()
	} else {
		usage()
		os.Exit(1)
	}
}

/* EOF */
