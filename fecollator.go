/* fecollator.go */


/* TODO:
  1. run next task after sync
  2. yield
  3. hold -> hold from the same
    sync & retry
  4. commit ahead from other
    sync & retry
  5. error behind or ahead
    sync & retry


  *. for debugging:
    curl -L http://localhost:30004/debug/pprof/goroutine?debug=2
*/

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

  committing-(got quorum)------>init
           +-(commit req)------>init or committing
           \-(others)---------->committing

  holding--(yield)----->holding
        +--(commit)---->init
        \--(timeout)--->init

  failure-->init

-- method names
  -- local
  sync
  put
  get
  status

  -- internal command
  timeout
  fin

  -- between peers
  connect <-> ack, error
  ping <-> pong
  get <-> data, error
  hold <-> holding, errro
  commit <-> committed, holding, error
  yield <-> yielded, error
  cancel <-> canceled, error

 */

package fe

import (
	"fmt"
	"strconv"
	"sync"
	_ "sync/atomic"
	"time"
)

type FeCollator struct {
	sync.RWMutex

	Channel		string
	Phase		uint64
	State		string

	task		*FeData
	data		*FeData
	proposal	*FeData

	me			string
	peers		map[string]bool	// true if self
	packets		[]*FeData		// received data packets

	target		int				// quorum count
	nSent		int32			// # of requests sent in this phase

	HoldDuration	int			// in seconds
	Timeout			int			// in milliseconds

	Tasks		chan *FeData	`json:"-"`

	caretakers	map[uint64]*FeData
	timeoutsLock *sync.Mutex
	timeouts	map[uint64]*time.Timer
}

type FeCollatorSummary struct {
	myCount			int
	quorum			*FeData
	quorumCount		int
	candidate		*FeData
	candidateCount	int
	nReceived		int

	needSync		bool
	yieldRequest	*FeData		// the best valid yield request
	commitRequest	*FeData		// the best valid commit request
}

var (
)

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

	fc.Tasks = make(chan *FeData, 128)
	fc.caretakers = map[uint64]*FeData{}
	fc.timeoutsLock = &sync.Mutex{}
	fc.timeouts = map[uint64]*time.Timer{}

	FeventRegister(fc.Channel, func(data interface{}) error {
		return fc.proc(data.(*FeData), false)
	})

	return fc
}

func (fc *FeCollator) setPeers(me *Peer, peers []*Peer) {
	fc.Lock()
	defer fc.Unlock()

	fc.peers = map[string]bool{}
	for _, i := range peers {
		fc.peers[i.Id] = i.Self
		if i.Self {
			fc.me = i.Id
		}
	}

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
	data.From = fc.me
	data.Channel = fc.Channel
	data.State = fc.State
	data.Phase = fc.Phase
	data.Index = fc.data.Index
	data.Issuer = fc.me
	return data
}

func (fc *FeCollator) prepError(data *FeData, err int) *FeData {
	data.Type = "error"
	data.From = fc.me
	data.Channel = fc.Channel
	data.State = fc.State
	data.Phase = fc.Phase
	data.Key = strconv.Itoa(err)
	data.Value = "error"
	return data
}

func (fc *FeCollator) setState(state string, phaseShift, resetPackets bool, pickupTask bool) {
	fc.State = state
	if phaseShift {
		fc.Phase++
	}
	fc.nSent = 0
	if resetPackets {
		fc.packets = nil
	}

	fmt.Printf("%s %s/%d/%d\n", fc.me, fc.State, fc.data.Index, fc.Phase)

	if state == "init" && pickupTask {
		fc.pickup(true)
	}
}

func (fc *FeCollator) pickup(locked bool) {
	if !locked {
		fc.Lock()
		defer fc.Unlock()
	}
	if fc.State == "init" {
		select {
		case task := <- fc.Tasks:
			go fc.proc(task, false)
		default:
			fc.task = nil
		}
	}
}

func (fc *FeCollator) setProposal(proposal *FeData) {
	fc.proposal = &FeData{}
	*fc.proposal = *proposal
}

func (fc *FeCollator) resetProposal() {
	fc.proposal = nil
}

func (fc *FeCollator) timeout(toms int, phase uint64, _type string) {
	if _type == "" {
		_type = "timeout"
	}
	if phase == 0 {
		phase = fc.Phase
	}

	var to FeData
	fc.prepData(&to, _type)
	to.Phase = phase

	fc.cancelTimeout(phase)
	fc.timeoutsLock.Lock()
	fc.timeouts[phase] = time.AfterFunc(time.Duration(toms) * time.Millisecond, func() {
		fc.timeoutsLock.Lock()
		delete(fc.timeouts, phase)
		fc.timeoutsLock.Unlock()
		fc.proc(&to, false)
	})
	fc.timeoutsLock.Unlock()
}

func (fc *FeCollator) cancelTimeout(phase uint64) {
	fc.timeoutsLock.Lock()
	defer fc.timeoutsLock.Unlock()
	timer, ok := fc.timeouts[phase]
	if ok {
		delete(fc.timeouts, phase)
		timer.Stop()
	}
}

func (fc *FeCollator) sendToPeers(data *FeData) error {
	for id, self := range fc.peers {
		if self {
			continue
		}

		err := sendTo(id, data)
		if err != nil {
			fmt.Printf("Failed to send to %s: %s\n", id, err)
			x := *feErrorSendFailure
			x.From = id
			x.Channel = fc.Channel
			x.State = fc.State
			x.Phase = fc.Phase
			fc.proc(&x, true)
		} else {
			//fmt.Printf("  Sent to    %s %s/%d/%d\n", id, data.Type, data.Index, data.Phase)
		}
/*
		go func(id string) {
			err := sendTo(id, data)
			if err != nil {
				fmt.Printf("Failed to send to %s: %s\n", id, err)
				x := *feErrorSendFailure
				x.From = id
				x.Channel = fc.Channel
				x.State = fc.State
				x.Phase = fc.Phase
				go fc.proc(&x, false)
			} else {
				fmt.Printf("  Sent to    %s %s/%d/%d\n", id, data.Type, data.Index, data.Phase)
			}
		}(id)
*/
	}

	return nil
}

func (fc *FeCollator) collate(summary *FeCollatorSummary) {
	var mine *FeData
	if fc.State == "proposing" || fc.State == "committing" {
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

			if i.Type == "error" {
				// TODO: check error code saying it's behind or something
				summary.needSync = true
			}
		}

		if fc.State == "proposing" && i.Type == "holding" {
			if fc.me != i.Issuer ||
				(fc.me == i.Issuer && fc.proposal.Index == i.Index &&
				fc.Phase == i.Phase) {
				valid = true
			}
		} else if fc.State == "committing" && i.Type == "committed" {
			if fc.me != i.Issuer ||
				(fc.me == i.Issuer && fc.proposal.Index == i.Index &&
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

	/* for yield, check count and hold-until */
	/* for commit, check index */

	//fmt.Printf("XXX: %+v\n", summary)
}

/* fc should be locked and state init */
func (fc *FeCollator) syncAndGo(nextTask *FeData) error {
	task := &FeData{}
	fc.prepData(task, "sync")
	task.next = nextTask
	return fc.proc(task, true)
}

/* handles
-- from local: put, sync, timeout, error
-- from remote: hold, commit, error
 */
func (fc *FeCollator) initProc(data *FeData) error {
	var req FeData

	switch data.Type {
	case "put":
		if data.From != me.Id {
			return fmt.Errorf("non-local put request")
		}
		/** TODO: check data validity */

		fc.task = data
		fc.setState("proposing", true, true, false)
		data.From = fc.me
		data.Channel = fc.Channel
		data.State = fc.State
		data.Phase = fc.Phase
		data.Index = fc.data.Index + 1
		data.Issuer = fc.me
		data.Timestamp = time.Now().UnixNano()
		data.HoldUntil = data.Timestamp + (time.Duration(fc.HoldDuration) * time.Second / time.Nanosecond).Nanoseconds()

		var data2, data3 FeData
		data2 = *data
		data2.Type = "hold"
		data3 = *data
		data3.Type = "holding"

		fc.setProposal(&data3)

		go fc.timeout(fc.Timeout, 0, "")
		//atomic.AddInt32(&fc.nSent, int32(len(fc.peers)))
		fc.nSent = int32(len(fc.peers))
		fc.proc(&data3, true)
		fc.sendToPeers(&data2)

	case "sync":
		if data.From != me.Id {
			return fmt.Errorf("invalid remote sync request")
		}

		/** TODO: check data validity */

		fc.task = data
		fc.setState("syncing", true, true, false)
		fc.prepData(&req, "get")
		req.Timestamp = time.Now().UnixNano()
		req.HoldUntil = req.Timestamp

		var ldata FeData
		ldata = *fc.data
		ldata.Type = "data"
		ldata.From = fc.me
		ldata.Phase = fc.Phase

		go fc.timeout(fc.Timeout, 0, "")
		//atomic.AddInt32(&fc.nSent, len(fc.peers))
		fc.nSent = int32(len(fc.peers))
		fc.proc(&ldata, true)
		fc.sendToPeers(&req)

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
			fc.prepError(&req, 2)
			req.Issuer = data.Issuer
			req.Channel = data.Channel
			req.State = data.State
			req.Phase = data.Phase
			req.Index = data.Index
			req.Value = "Index behind"
			return sendTo(data.From, &req)
		} else if data.Index > fc.data.Index + 1 {
			// am i behind? sync and go
			fc.prepError(&req, 4)
			req.Issuer = data.Issuer
			req.Channel = data.Channel
			req.State = data.State
			req.Phase = data.Phase
			req.Index = data.Index
			req.Value = "Index ahead"
			return sendTo(data.From, &req)
			//return fc.syncAndGo(data)
		} else if data.HoldUntil <= ct {
			// it's expired already?
			fc.prepError(&req, 3)
			req.Issuer = data.Issuer
			req.Channel = data.Channel
			req.State = data.State
			req.Phase = data.Phase
			req.Index = data.Index
			req.Value = "Wrong time"
			return sendTo(data.From, &req)
		} else {
			// we're good

			fc.setState("holding", true, true, false)
			fc.setProposal(data)

			fc.prepData(&req, "holding")
			req.Issuer = data.Issuer
			req.Channel = data.Channel
			req.State = data.State
			req.Phase = data.Phase
			req.Index = data.Index

			err := sendTo(data.From, &req)
			if err == nil {
				go fc.timeout(int((data.HoldUntil - ct) / 1000), 0, "")
			}
			return err
		}

	case "get":
		req = *fc.data
		req.Type = "data"
		req.From = fc.me
		return sendTo(data.From, &req)

	case "commit":
		fmt.Printf("%s: Got obsolete commit: %+v\n", fc.me, data)
		// maybe need to start syncing

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

			if fc.task.tracker != nil {
				fc.task.tracker(summary.quorum, "done", nil)
			}
			//fmt.Printf("Got quorum: %+v\n%+v\n", fc.data, summary)
		} else {
			if fc.task.tracker != nil {
				var rsp FeData
				fc.prepError(&rsp, 1)
				rsp.Value = "Quorum not reached"
				fc.task.tracker(&rsp, "canceled", nil)
			}
			//fmt.Printf("Quorum not reached: %+v\n", summary)
		}

		if fc.task.next == nil {
			fc.putCaretaker(fc.task)
			fc.setState("init", true, true, true)
		} else {
			fc.putCaretaker(fc.task)
			task := fc.task.next
			fc.setState("init", true, true, false)
			return fc.proc(task, true)
		}
	}
	return nil
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
    timeout
*/
func (fc *FeCollator) proposingProc(data *FeData) error {
	if data.Channel != fc.Channel {
		return fmt.Errorf("wrong channel")
	}

	if data.Type != "timeout" {
/*
		fmt.Printf("  Got from   %s %s/%d/%d\n", data.From, data.Type,
			data.Index, data.Phase)
*/
	}

	done := false
	switch data.Type {
	case "timeout":
		done = true

	case "error":
		fc.packets = append(fc.packets, data)

	case "holding":
		var ndata *FeData
		if data.Issuer == fc.me {
			ndata = &FeData{}
			*ndata = *fc.proposal
			ndata.From = data.From
		} else {
			ndata = data
		}
		fc.packets = append(fc.packets, ndata)

	case "yield":
		// ignore for now

	default:
		fmt.Printf("%s XXX: %+v\n", fc.me, data)
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

	if done {
		if summary.myCount < fc.target {
			if true {
				/* sync and try again */
				var ndata FeData
				fc.prepData(&ndata, "cancel")
				fc.sendToPeers(&ndata)

				fmt.Printf("quorum not reached, sync and go\n")

				task := fc.task
				fc.putCaretaker(fc.task)
				fc.setState("init", true, true, false)
				fc.syncAndGo(task)

/*
				if fc.task.tracker != nil {
					fc.task.tracker(summary.quorum, "canceled", nil)
				}

				fc.putCaretaker(fc.task)
				fc.setState("init", true, true, true)

				fmt.Printf("quorum not reached\n")
*/
			} else {
				/* cancel them */
				var ndata FeData
				fc.prepData(&ndata, "cancel")
				fc.sendToPeers(&ndata)

				if fc.task.tracker != nil {
					fc.task.tracker(summary.quorum, "canceled", nil)
				}

				fc.putCaretaker(fc.task)
				fc.setState("init", true, true, true)

				//fmt.Printf("quorum not reached\n")
			}
		} else {
			/* commit them */

			fc.setState("committing", false, true, true)

			fc.proposal.Type = "data"
			fc.proposal.From = fc.me
			fc.proposal.State = "committed"
			fc.proposal.HoldUntil = fc.proposal.Timestamp
			fc.setData(fc.proposal)

			var data2, data3 FeData
			data2 = *fc.data
			data2.Type = "committed"
			data3 = *fc.data
			data3.Type = "commit"

			fc.nSent = int32(len(fc.peers))
			//atomic.AddInt32(&fc.nSent, len(fc.peers))
			fc.proc(&data2, true)
			fc.sendToPeers(&data3)
		}
	}
	return nil
}

// committed, error
func (fc *FeCollator) committingProc(data *FeData) error {
	if data.Channel != fc.Channel {
		return fmt.Errorf("wrong channel")
	}

	if data.Type != "timeout" {
/*
		fmt.Printf("  Got from   %s %s/%d/%d\n", data.From, data.Type,
			data.Index, data.Phase)
*/
	}

	done := false
	switch data.Type {
	case "timeout":
		if data.Phase != fc.Phase {
			return nil
		}
		done = true

	case "holding":
		// ignore
		return nil

	case "error":
		fc.packets = append(fc.packets, data)

	case "committed":
		var ndata *FeData
		if data.Issuer == fc.me {
			ndata = &FeData{}
			*ndata = *fc.data
			ndata.From = data.From
			ndata.Type = data.Type
		} else {
			ndata = data
		}
		fc.packets = append(fc.packets, ndata)

	default:
		fmt.Printf("%s XXX: %+v\n", fc.me, data)
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

	if done {
		if summary.myCount < fc.target {
			if fc.task.tracker != nil {
				fc.task.tracker(summary.quorum, "canceled", nil)
			}
			fmt.Printf("%+v\n", summary)
			fmt.Printf("Quorum not reached while committing!\n")
		} else {
			// all good
			if fc.task.tracker != nil {
				ndata2 := *fc.data
				ndata2.Type = "committed"
				fc.task.tracker(&ndata2, "committed", nil)
			}
		}

		fc.resetProposal()
		fc.putCaretaker(fc.task)
		fc.setState("init", true, true, true)
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
			rsp.Index = fc.proposal.Index
			rsp.Issuer = data.Issuer
			rsp.Phase = data.Phase
			sendTo(data.From, &rsp)

			x := fc.proposal
			fc.resetProposal()

			x.Type = "data"
			x.State = "committed"
			fc.setData(x)

			fc.setState("init", true, true, true)
		} else {
			fmt.Printf("%s: in %s, ignoring %+v, %+v\n", fc.me, fc.State,
				data, fc.proposal)
		}

	case "cancel":
		if data.From == data.Issuer && data.Issuer == fc.proposal.Issuer &&
			data.Index == fc.proposal.Index {
			fc.prepData(&rsp, "canceled")
			rsp.Issuer = data.Issuer
			rsp.Phase = data.Phase
			sendTo(data.From, &rsp)

			fc.resetProposal()
			fc.setState("init", true, true, true)
		} else {
			fmt.Printf("%s: in %s, ignoring %+v, %+v\n", fc.me, fc.State,
				data, fc.proposal)
		}

	default:
		fmt.Printf("%s: in %s, ignoring %+v, %+v\n", fc.me, fc.State,
			data, fc.proposal)
	}

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
		sendTo(data.From, &rsp)

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

func (fc *FeCollator) proc(data *FeData, locked bool) error {
	if !locked {
		fc.Lock()
		defer fc.Unlock()
	}

	if fc.State == "holding" {
		ct := time.Now().UnixNano()
		if fc.proposal.HoldUntil < ct || fc.proposal.HoldUntil > ct + (3600 *time.Second / time.Nanosecond).Nanoseconds() {
			// dump
			fmt.Printf("%s: dumping proposal: ts=%d hold=%d ct=%d\n", fc.me, fc.proposal.Timestamp, fc.proposal.HoldUntil, ct)
			fc.resetProposal()
			fc.setState("init", true, true, true)
		}
	}

	var err error

	/* handle stateless generics */
	switch data.Type {
	case "ping": fallthrough
	case "pong": fallthrough
	case "get": fallthrough
	case "status":
		return fc.statelessProc(data)
	}

	/* handle replies or timeouts */
	switch data.Type {
	case "timeout": fallthrough
	case "fin": fallthrough
	case "data": fallthrough
	case "error": fallthrough
	case "committed": fallthrough
	case "holding": fallthrough
	case "yielded": fallthrough
	case "canceled":
		if data.Phase < fc.Phase {
			task, ok := fc.caretakers[data.Phase]
			if ok {
				return fc.takeAfter(task, data)
			} else {
				return nil
			}
		}
	}

	switch fc.State {
	case "init":
		err = fc.initProc(data)
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

/*
	if after != nil && err != nil {
		var rsp FeData
		fc.prepError(&rsp, 1)
		rsp.Value = err.Error()
		after(&rsp)
	}
*/

	return err
}

func (fc *FeCollator) takeAfter(task *FeData, data *FeData) error {
	if data.Type == "fin" {
		// remove itself
		//fmt.Printf("%s finishing %s/%d\n", fc.me, task.Type, task.Phase)
		delete(fc.caretakers, task.Phase)
		return nil
	}

	switch task.Type {
	case "sync":
/*
		fmt.Printf("%s syncing/%d got %s from %s after\n", fc.me, task.Phase,
			data.Type, data.From)
*/
	case "put":
/*
		if data.Type != "timeout" {
			fmt.Printf("  After from %s %s/%d/%d\n", data.From, data.Type,
				data.Index, data.Phase)
		}
*/
		/*
		fmt.Printf("%s putting/%d got %s from %s after\n", fc.me, task.Phase,
			data.Type, data.From)
*/
	}

	return nil
}

func (fc *FeCollator) putCaretaker(task *FeData) {
	var caretaker FeData
	caretaker = *task
	caretaker.Phase = fc.Phase
	fc.caretakers[caretaker.Phase] = &caretaker
	go fc.timeout(fc.Timeout, caretaker.Phase, "fin")
}

func (fc *FeCollator) putTask(task *FeData) error {
	fc.Tasks <- task
	fc.pickup(false)
	return nil
}

/* EOF */
