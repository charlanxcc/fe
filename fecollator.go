/* fecollator.go */

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
          +--(other candiate)-->holding
          +--(yield req)------->holding
          +--(commit req)------>proposing
          +--(no quorum)------->failure
          \--(others)---------->proposing

  holding--(yield)----->holding
        +--(commit)---->init
        \--(timeout)--->init

  commiting--(regardless)-->init

  failure-->init

-- method names
  -- local
  sync
  put
  get
  status

  -- internal command
  timeout

  -- between peers
  connect <-> ack, error
  ping <-> pong
  get <-> data, error
  hold <-> holding, errro
  commit <-> committed, holding, error
  commit-full <-> committed, holding, error
  yield <-> yielded, error
  cancel <-> canceled, error

 */

package fe

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type FeCollator struct {
	sync.RWMutex

	Channel		string
	Phase		uint64
	State		string

	data		*FeData
	proposal	*FeData

	me			*Peer
	peers		[]*Peer
	psid		map[string]*Peer
	packets		[]*FeData		// received data packets

	target		int				// quorum count
	nSent		int32			// # of requests sent in this phase

	HoldDuration	int			// in seconds
	Timeout			int			// in milliseconds

	Jobs		chan *FeData
	after		func(*FeData)
}

type FeCollatorSummary struct {
	myCount			int
	quorum			*FeData
	quorumCount		int
	candidate		*FeData
	candidateCount	int
	nReceived		int

	yieldRequest	*FeData		// the best valid yield request
	commitRequest	*FeData		// the best valid commit request
}

var (
)

func feCollatorHandler(param interface{}, data interface{}) error {
	return param.(*FeCollator).proc(data.(*FeData), nil)
}

func NewFeCollator(channel string) *FeCollator {
	fc := &FeCollator{
		Channel: channel,
		State: "init",
		Phase: 1,
		data: nil,
		proposal: nil,
		HoldDuration: 30,
		Timeout: 5000,
	}

	fc.data = &FeData{}
	*fc.data = *feDataGenesis
	fc.data.Channel = fc.Channel

	FeventRegister(fc.Channel, feCollatorHandler, fc)
	return fc
}

func (fc *FeCollator) setPeers(me *Peer, peers []*Peer) {
	fc.Lock()

	fc.psid = map[string]*Peer{}

	fc.me = me
	fc.peers = append(fc.peers, peers...)
	for _, i := range fc.peers {
		fc.psid[i.Id] = i
	}

	fc.Unlock()

	fc.target = len(fc.peers) / 2
	if len(fc.peers) % 2 != 0 {
		fc.target++
	}
}

func (fc *FeCollator) setData(data *FeData) {
	*fc.data = *data
}

func (fc *FeCollator) prepData(data *FeData, _type string) *FeData {
	data.Type = _type
	data.From = fc.me.Id
	data.Channel = fc.Channel
	data.State = fc.State
	data.Phase = fc.Phase
	data.Index = fc.data.Index
	data.Issuer = fc.me.Id
	return data
}

func (fc *FeCollator) prepError(data *FeData, err int) *FeData {
	data.Type = "error"
	data.From = fc.me.Id
	data.Channel = fc.Channel
	data.State = fc.State
	data.Phase = fc.Phase
	data.Key = strconv.Itoa(err)
	data.Value = "error"
	return data
}

func (fc *FeCollator) setState(state string, resetPackets bool) {
	fc.State = state
	fc.Phase++
	fc.nSent = 0
	if resetPackets {
		fc.packets = nil
	}
}

func (fc *FeCollator) setProposal(proposal *FeData) {
	fc.proposal = &FeData{}
	*fc.proposal = *proposal
}

func (fc *FeCollator) resetProposal() {
	fc.proposal = nil
}

func (fc *FeCollator) timeout(toms int) {
	var to FeData
	time.Sleep(time.Duration(toms) * time.Millisecond)
	fc.proc(fc.prepData(&to, "timeout"), nil)
}

func (fc *FeCollator) sendTo(id string, data *FeData) error {
	if id == fc.me.Id {



	}

	p, ok := fc.psid[id]
	if !ok {
		return fmt.Errorf("invalid peer id")
	} else {
		return p.sendJson(data)
	}
}

func (fc *FeCollator) sendToPeers(data *FeData) error {
	for _, p := range fc.peers {
		if p.Self || !p.State {
			continue
		}

		atomic.AddInt32(&fc.nSent, 1)
		go func(p *Peer) {
			err := p.sendJson(data)
			if err != nil {
				fmt.Printf("Failed to send to %s: %s\n", p.Id, err)
				x := *feErrorSendFailure
				x.From = p.Id
				x.Channel = fc.Channel
				x.State = fc.State
				x.Phase = fc.Phase
				fc.proc(&x, nil)
			}
		}(p)
	}

	return nil
}

/* handles
-- from local: put, sync, timeout, error
-- from remote: hold, commit-full, error
 */
func (fc *FeCollator) initProc(data *FeData, after func(*FeData)) error {
	var req FeData

	switch data.Type {
	case "put":
		if data.From != me.Id {
			return fmt.Errorf("local hold request")
		}
		/** TODO: check data validity */

		if after != nil {
			fc.after = after
		}
		fc.setState("proposing", true)
		data.Type = "hold"
		data.From = fc.me.Id
		data.Channel = fc.Channel
		data.State = fc.State
		data.Phase = fc.Phase
		data.Index = fc.data.Index + 1
		data.Issuer = fc.me.Id
		data.Timestamp = time.Now().UnixNano()
		data.HoldUntil = data.Timestamp + (time.Duration(fc.HoldDuration) * time.Second / time.Nanosecond).Nanoseconds()

		var data2 FeData
		data2 = *data
		data2.Type = "holding"

		fc.setProposal(&data2)
		atomic.AddInt32(&fc.nSent, 1)

		fc.sendToPeers(data)
		go fc.proc(&data2, nil)
		go fc.timeout(fc.Timeout)

	case "sync":
		if data.From != me.Id {
			return fmt.Errorf("invalid remote sync request")
		}

		/** TODO: check data validity */

		if after != nil {
			fc.after = after
		}
		fc.setState("syncing", true)
		fc.prepData(&req, "get")
		req.Timestamp = time.Now().UnixNano()
		req.HoldUntil = req.Timestamp

		atomic.AddInt32(&fc.nSent, 1)
		var ldata FeData
		ldata = *fc.data
		ldata.Type = "data"
		ldata.From = fc.me.Id
		ldata.Phase = fc.Phase

		fc.sendToPeers(&req)
		go fc.proc(&ldata, nil)
		go fc.timeout(fc.Timeout)

	case "timeout":
		// ignore

	case "error":
		// ignore

	case "hold":
		if data.From == me.Id {
			return fmt.Errorf("local hold request")
		}
		/** TODO: check data validity */

		ct := time.Now().UnixNano()
		if data.Index <= fc.data.Index {
			// obsolete
		} else if data.Index > fc.data.Index + 1 {
			// am i behind?
		} else if data.HoldUntil <= ct {
			// it's expired already?
		} else {
			// we're good

			fc.setState("holding", true)
			fc.setProposal(data)

			fc.prepData(&req, "holding")
			req.Issuer = data.Issuer
			req.Channel = data.Channel
			req.State = data.State
			req.Phase = data.Phase
			req.Index = data.Index

			err := fc.sendTo(data.From, &req)
			if err == nil {
				go fc.timeout(int((data.HoldUntil - ct) / 1000))
			}
			return err
		}

	case "get":
		req = *fc.data
		req.Type = "data"
		req.From = fc.me.Id

		return fc.sendTo(data.From, &req)

	case "commit":
		fmt.Printf("Got obsolete commit: %+v\n", data)
		// maybe need to start syncing

	case "commit-full":
		fmt.Printf("%s ###: %+v\n", fc.me.Id, data)

		if data.Index != fc.data.Index + 1 {
			fmt.Printf("Got out of sync commit-full: %+v\n", data)
			// maybe need to start syncing
		} else if data.Index == fc.data.Index + 1 {
			data.HoldUntil = data.Timestamp
			fc.setData(data)

			req = *data
			req.Type = "committed"
			req.Key = ""
			req.Value = ""

			err := fc.sendTo(data.From, &req)
			fc.resetProposal()
			fc.setState("init", true)
			return err
		}

	default:
		fmt.Printf("Got obsolete: %+v\n", data)
	}
	return nil
}

func (fc *FeCollator) syncingProc(data *FeData) error {
	if data.Channel != fc.Channel {
		return fmt.Errorf("wrong channel")
	}

	// timeout, error, data,
	done := false
	switch data.Type {
	case "timeout":
		done = true

	case "error":
		// ignore

	case "data":
		if fc.Phase == data.Phase {
			fc.packets = append(fc.packets, data)
		}
	}

	var summary FeCollatorSummary
	fc.collate(&summary)

	if !done && summary.nReceived >= int(fc.nSent) {
		done = true
		fc.collate(&summary)
	} else if summary.nReceived >= fc.target {
		fc.collate(&summary)
		if summary.quorum != nil {
			done = true
		}
	}

	if done {
		if summary.quorum != nil {
			fc.setData(summary.quorum)
			if fc.after != nil{
				fc.after(summary.quorum)
			}
			fmt.Printf("Got quorum: %+v\n%+v\n", fc.data, summary)
		} else {
			if fc.after != nil {
				var rsp FeData
				fc.prepError(&rsp, 1)
				rsp.Value = "Quorum not reached"
				fc.after(&rsp)
			}
			fmt.Printf("Quorum not reached: %+v\n", summary)
		}

		fc.after = nil
		fc.setState("init", true)
	}
	return nil
}

func (fc *FeCollator) collate(summary *FeCollatorSummary) {
	var mine *FeData
	if fc.State == "proposing" {
		mine = fc.proposal
	}

	// id string -> { data *FeData, count int }
	m := map[string][2]interface{}{}
	// id string -> count int
	mm := map[string]int{}

	for _, i := range fc.packets {
		valid := false

		if i.Phase == fc.Phase {
			mm[i.From]++
		}

		if fc.State == "proposing" && i.Type == "holding" {
			if fc.me.Id != i.Issuer ||
				(fc.me.Id == i.Issuer && fc.proposal.Index == i.Index &&
				fc.Phase == i.Phase) {
				valid = true
			}
		} else if fc.State == "syncing" && i.Type == "data" &&
			fc.Phase == i.Phase {
			valid = true
		}

		if !valid {
			continue
		}

		id := i.Id()
		v, ok := m[id]
		if ok {
			*v[1].(*int) += 1
		} else {
			var ii = 1
			w := [2]interface{}{i, &ii}
			m[id] = w
		}
	}

	var mid string = ""
	if mine != nil {
		mid = mine.Id()
	}

	x, ok := m[mid]
	if !ok {
		summary.myCount = 0;
	} else {
		summary.myCount = *x[1].(*int)
	}

	var q, o *FeData
	var qn, on int = 0, 0
	for i, j := range m {
		if q == nil || *j[1].(*int) > qn {
			q  = j[0].(*FeData)
			qn = *j[1].(*int)
		}
		if i != mid && (o == nil || *j[1].(*int) > on) {
			o  = j[0].(*FeData)
			on = *j[1].(*int)
		}
	}

	if qn < fc.target {
		q  = nil
		qn = 0
	}
	if on < summary.myCount ||
		(mine != nil && o != nil && on == summary.myCount &&
		o.Timestamp > mine.Timestamp) {
		o  = nil
		on = 0
	}

	summary.nReceived      = len(mm)
	summary.quorum         = q
	summary.quorumCount    = qn
	summary.candidate      = o
	summary.candidateCount = on

	fmt.Printf("%+v\n", m)

	/* for yield, check count and hold-until */
	/* for commit, check index */
}

/*
  proposing--(got quorum)------>commiting
          +--(we candidate)---->proposing
          +--(other candiate)-->yielding
          +--(yiled req)------->yielding
          +--(commit req)------>proposing
          +--(no quorum)------->failure
          \--(others)---------->proposing

  handles:
    holding
    error
    yield
    commit
    commit-full
    timeout
*/
func (fc *FeCollator) proposingProc(data *FeData) error {
	if data.Channel != fc.Channel {
		return fmt.Errorf("wrong channel")
	}

	done := false
	switch data.Type {
	case "timeout":
		done = true

	case "error": fallthrough
	case "holding":
		if fc.Phase == data.Phase {
			var ndata *FeData
			if data.Issuer == fc.me.Id {
				ndata = &FeData{}
				*ndata = *fc.proposal
				ndata.From = data.From
			} else {
				ndata = data
			}
			fc.packets = append(fc.packets, ndata)
		}

	case "yield":
		// ignore for now

	default:
		fmt.Printf("%s XXX: %+v\n", fc.me.Id, data)
		return nil
	}

	var summary FeCollatorSummary
	fc.collate(&summary)

	if !done && summary.nReceived >= int(fc.nSent) {
		done = true
		fc.collate(&summary)
	} else if summary.nReceived >= fc.target {
		fc.collate(&summary)
		if summary.quorum != nil {
			done = true
		}
	}

	fmt.Printf("done=%s %+v\n", done, summary)

	if done {
		if summary.myCount < fc.target {
			/* cancel them */
			var ndata FeData
			fc.prepData(&ndata, "cancel")
			fc.sendToPeers(&ndata)

			if fc.after != nil {
				var rsp FeData
				fc.prepError(&rsp, 1)
				rsp.Value = "Quorum not reached"
				fc.after(&rsp)
			}

			fmt.Printf("quorum not reached\n")
		} else {
			/* commit them */

			fc.proposal.Type = "data"
			fc.proposal.From = fc.me.Id
			fc.proposal.State = "committed"
			fc.proposal.HoldUntil = fc.proposal.Timestamp
			fc.setData(fc.proposal)
			fc.resetProposal()

			ndata := *fc.data
			ndata.Type = "commit"
			fc.sendToPeers(&ndata)

			if fc.after != nil {
				fc.after(fc.data)
			}
		}

		fc.after = nil
		fc.setState("init", true)
	}
	return nil
}

// commit, cancel
func (fc *FeCollator) holdingProc(data *FeData) error {
	var rsp FeData

	if data.Channel != fc.Channel {
		return fmt.Errorf("wrong channel")
	}

	switch data.Type {
	case "commit":
		if data.From == data.Issuer && data.Issuer == fc.proposal.Issuer &&
			data.Index == fc.proposal.Index {
			fc.prepData(&rsp, "committed")
			rsp.Issuer = data.Issuer
			rsp.Phase = data.Phase
			fc.sendTo(data.From, &rsp)

			x := fc.proposal
			fc.resetProposal()

			x.Type = "data"
			x.State = "committed"
			fc.setData(x)

			fc.setState("init", true)
		} else {
			fmt.Printf("Ignoring %+v, %+v\n", data, fc.proposal)
		}

	case "cancel":
		if data.From == data.Issuer && data.Issuer == fc.proposal.Issuer &&
			data.Index == fc.proposal.Index {
			fc.prepData(&rsp, "canceled")
			rsp.Issuer = data.Issuer
			rsp.Phase = data.Phase
			fc.sendTo(data.From, &rsp)

			fc.resetProposal()
			fc.setState("init", true)
		} else {
			fmt.Printf("Ignoring %+v, %+v\n", data, fc.proposal)
		}

	default:
		fmt.Printf("Ignoring %+v\n", data)
	}

	return nil
}

func (fc *FeCollator) committingProc(data *FeData) error {
	return nil
}

func (fc *FeCollator) yieldingProc(data *FeData) error {
	return nil
}

func (fc *FeCollator) statelessProc(data *FeData) error {
	var rsp FeData

	switch data.Type {
	case "ping":

	case "pong":

	case "get":
		rsp = *fc.data
		rsp.Phase = data.Phase
		fc.sendTo(data.From, &rsp)

	case "commit-full":
		if data.Index <= fc.data.Index {
			fc.prepError(&rsp, 1)
			rsp.Value = "wrong index"
			rsp.Phase = data.Phase
			fc.sendTo(data.From, &rsp)
		} else {
			fc.prepData(&rsp, "committed")
			rsp.Issuer = data.Issuer
			rsp.Phase = data.Phase
			fc.sendTo(data.From, &rsp)

			data.Type = "data"
			data.State = "committed"
			fc.setData(data)

			if fc.State == "proposing" || fc.State == "holding" {
				fc.setState("init", true)
			}
		}
/*
	case "status":
		fc.prepData(&rsp, "data")
		rsp.Phase = data.Phase
		rsp.Value, _ = status()
		fc.sendTo(data.From, &rsp)
*/
	}
	return nil
}

func (fc *FeCollator) proc(data *FeData, after func(*FeData)) error {
	fc.Lock()
	defer fc.Unlock()

	var err error

	/* handle stateless generics */
	switch data.Type {
	case "ping": fallthrough
	case "pong": fallthrough
	case "get": fallthrough
	case "commit-full": fallthrough
	case "status":
		return fc.statelessProc(data)
	}

	switch fc.State {
	case "init":
		err = fc.initProc(data, after)
	case "syncing":
		err = fc.syncingProc(data)
	case "proposing":
		err = fc.proposingProc(data)
	case "holding":
		err = fc.holdingProc(data)
	case "committing":
		err = fc.committingProc(data)
	default:
		err = fmt.Errorf("Unknown state %s\n", fc.State)
	}

	if after != nil && err != nil {
		var rsp FeData
		fc.prepError(&rsp, 1)
		rsp.Value = err.Error()
		after(&rsp)
	}

	return err
}

/* EOF */
