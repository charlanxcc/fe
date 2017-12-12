/* fe-client.go */

package fe

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
)

var (
	FeClientConnectionError error = fmt.Errorf("peer connection protocol error")
)

func FeClientConnect(addr string) (net.Conn, error) {
	var req, rsp FeData
	var err error

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	req = FeData{
		Type: "client",
		Key: "id",
		Value: "client",
	}
	err = sendJson(conn, req)
	if err != nil {
		conn.Close()
		return nil, err
	}
	err = recvJson(conn, &rsp)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if rsp.Type != "ack" {
		conn.Close()
		return nil, FeClientConnectionError
	}

	return conn, nil
}

func FeClient() {
	var req, rsp FeData
	var conn net.Conn
	var err error

	addr := os.Args[2]
	conn, err = FeClientConnect(addr)
	if err != nil {
		fmt.Printf("Cannot connect to server %s: %s\n", os.Args[2], err)
		return
	}

	cmd := os.Args[1]
	switch cmd {
	case "sync":
		req = FeData{
			Type: "sync",
		}
		err = sendJson(conn, req)
		if err != nil {
			fmt.Printf("Failed to send to server %s: %s\n", addr, err)
			return
		}
		err = recvJson(conn, &rsp)
		if err != nil {
			fmt.Printf("Failed to recv from server %s: %s\n", addr, err)
			return
		}

		switch rsp.Type {
		case "error":
			fmt.Printf("Got error: %s/%s\n", rsp.Key, rsp.Value)
		case "data":
			fmt.Printf("%+v\n", rsp)
		default:
			fmt.Printf("Got unexpected data: %+v\n", rsp)
		}

	case "get":
		req = FeData{
			Type: "get",
		}
		err = sendJson(conn, req)
		if err != nil {
			fmt.Printf("Failed to send to server %s: %s\n", addr, err)
			return
		}
		err = recvJson(conn, &rsp)
		if err != nil {
			fmt.Printf("Failed to recv from server %s: %s\n", addr, err)
			return
		}

		switch rsp.Type {
		case "error":
			fmt.Printf("Got error: %s/%s\n", rsp.Key, rsp.Value)
		case "data":
			fmt.Printf("%+v\n", rsp)
		default:
			fmt.Printf("Got unexpected data: %+v\n", rsp)
		}

	case "put":
		req = FeData{
			Type: "put",
			Key: os.Args[3],
			Value: os.Args[4],
		}
		err = sendJson(conn, req)
		if err != nil {
			fmt.Printf("Failed to send to server %s: %s\n", addr, err)
			return
		}
		err = recvJson(conn, &rsp)
		if err != nil {
			fmt.Printf("Failed to recv from server %s: %s\n", addr, err)
			return
		}

		switch rsp.Type {
		case "error":
			fmt.Printf("Got error: %s/%s\n", rsp.Key, rsp.Value)
		case "data": fallthrough
		case "committed":
			fmt.Printf("%+v\n", rsp)
		default:
			fmt.Printf("Got unexpected data: %+v\n", rsp)
		}

	case "bulk-put":
		prefix := os.Args[3]
		start, err1 := strconv.Atoi(os.Args[4])
		end, err2 := strconv.Atoi(os.Args[5])
		if err1 != nil || err2 != nil {
			usage()
			return
		}

		for i := start; i <= end; i++ {
			req = FeData{
				Type: "put",
				Key: fmt.Sprintf("%s-%d", prefix, i),
				Value: fmt.Sprintf("%s-%d", prefix, i),
			}
			err = sendJson(conn, req)
			if err != nil {
				fmt.Printf("Failed to send to server %s: %s\n", addr, err)
				return
			}
			err = recvJson(conn, &rsp)
			if err != nil {
				fmt.Printf("Failed to recv from server %s: %s\n", addr, err)
				return
			}

			switch rsp.Type {
			case "error":
				fmt.Printf("Got error: %s/%s\n", rsp.Key, rsp.Value)
			case "data": fallthrough
			case "committed":
				fmt.Printf("%+v\n", rsp)
			default:
				fmt.Printf("Got unexpected data: %+v\n", rsp)
				os.Exit(1)
			}
		}

	case "status":
		req = FeData{Type: "status"}
		err = sendJson(conn, req)
		if err != nil {
			fmt.Printf("Failed to send to server %s: %s\n", addr, err)
			return
		}
		err = recvJson(conn, &rsp)
		if err != nil {
			fmt.Printf("Failed to recv from server %s: %s\n", addr, err)
			return
		}

		var q interface{}
		err = json.Unmarshal([]byte(rsp.Value), &q)
		if err != nil {
			fmt.Printf("Got broken json: %s\n", rsp.Value)
		}
		b, err := json.MarshalIndent(q, "", "  ")
		if err != nil {
			fmt.Printf("Got broken json: %s\n", rsp.Value)
		}
		fmt.Println(string(b))

	default:
		fmt.Printf("Unknown command %s\n", cmd)
	}

	return
}

/* EOF */
