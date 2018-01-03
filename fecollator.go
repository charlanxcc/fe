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
        +----(no quorum)--->syncing
        \----(hold, cancel, yield)->syncing (after putting them in queue)

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
	"bytes"
	"fmt"
	"sort"
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
	target			int
	nSent			int
	nReceived		int

	done			bool

	myCount			int
	quorum			*FeData
	quorumCount		int
	candidate		*FeData
	candidateCount	int

	needSync		bool
	yieldRequest	*FeData		// the best valid yield request
	commitRequest	*FeData		// the best valid commit request

	summary			string
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
		HoldDuration: 100,
		Timeout: 50000,
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

func (fc *FeCollator) _prep(dst *FeData, src *FeData, _type string, err int) {
	if src != nil {
		dst.Type = src.Type
		dst.From = src.From
		dst.Channel = src.Channel
		dst.State = src.State
		dst.Phase = src.Phase
		dst.Index = src.Index
		dst.Issuer = src.Issuer
		dst.Timestamp = src.Timestamp
		dst.HoldUntil = src.HoldUntil
	} else {
		dst.Type = _type
		dst.From = fc.me
		dst.Channel = fc.Channel
		dst.State = fc.State
		dst.Phase = fc.Phase
		dst.Index = fc.data.Index
		dst.Issuer = fc.me
	}

	if _type != "" {
		dst.Type = _type
	}
	if _type == "error" {
		dst.Key = strconv.Itoa(err)
		dst.Value = "error"
	}
}

func (fc *FeCollator) prep(_type string) *FeData {
	r := new(FeData)

	switch _type {
	case "get":			fallthrough
	case "put":			fallthrough
	case "sync":
		fc._prep(r, nil, _type, 0)

	case "hold":		fallthrough
	case "holding":		fallthrough
	case "commit":		fallthrough
	case "committed":	fallthrough
	case "yield":		fallthrough
	case "yielded":		fallthrough
	case "cancel":		fallthrough
	case "canceled":
		fc._prep(r, fc.proposal, _type, 0)

	case "timeout":		fallthrough
	case "fin":
		fc._prep(r, fc.proposal, _type, 0)
	}
	return r
}

func (fc *FeCollator) prepError(src *FeData, err int) *FeData {
	r := new(FeData)
	fc._prep(r, src, "error", err)
	return r
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

	index := fc.data.Index
	if state == "proposing" || state == "committing" {
		index++
	} else if state == "holding" {
		index = fc.proposal.Index
	}
	fmt.Printf("%s %s/%d/%d\n", fc.me, fc.State, index, fc.Phase)

	if state == "init" && pickupTask {
		go fc.pickup(false)
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
			//fmt.Printf("### task: %s/%s\n", task.Type, task.Key)
			fc.proc(task, true)
		default:
			fc.task = nil
		}
	}
}

func (fc *FeCollator) setProposal(proposal *FeData) {
	fc.proposal = new(FeData)
	*fc.proposal = *proposal
}

func (fc *FeCollator) resetProposal() {
	fc.proposal = nil
}

func (fc *FeCollator) timeout(toms int, phase uint64, _type string) {
	if _type == "" {
		_type = "timeout"
	}

	to := fc.prep(_type)
	if phase != 0 {
		to.Phase = phase
	} else {
		to.Phase = fc.Phase
	}

	fc.cancelTimeout(phase)
	fc.timeoutsLock.Lock()
	fc.timeouts[phase] = time.AfterFunc(time.Duration(toms) * time.Millisecond, func() {
		fc.timeoutsLock.Lock()
		delete(fc.timeouts, phase)
		fc.timeoutsLock.Unlock()
		fc.proc(to, false)
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
	Id := func(d *FeData) string {
		return fmt.Sprintf("%s.%d", d.Issuer, d.Index)
	}

	var mine *FeData
	var mid string
	if fc.State == "proposing" || fc.State == "committing" {
		mine = fc.proposal
		mid = Id(mine)
	}

	type Aggregate struct {
		data	*FeData
		count	int
	}

	// data-id string -> *Aggregate
	ma := map[string]*Aggregate{}
	// peer-id string -> *FeData: the latest packet
	mp := map[string]*FeData{}
	// ordered Aggregates
	var mc []*Aggregate

	for _, i := range fc.packets {
		valid := false

		if i.Phase == fc.Phase {
			mp[i.From] = i
			if i.Type == "error" {
				// TODO: check error code saying it's behind or something
				summary.needSync = true
			}
		}

		if fc.State == "syncing" && i.Type == "data" &&
			fc.Phase == i.Phase {
			valid = true
		} else if (fc.State == "proposing" && i.Type == "holding") ||
			(fc.State == "committing" && i.Type == "committed") {
			if fc.me != i.Issuer ||
				(fc.me == i.Issuer && fc.proposal.Index == i.Index &&
				fc.Phase == i.Phase) {
				valid = true
			}
		}

		if !valid {
			continue
		}

		id := Id(i)
		v, ok := ma[id]
		if ok {
			v.count++
		} else {
			v := &Aggregate{count: 1}
			if id == mid {
				v.data = mine
			} else {
				v.data = i
			}
			ma[id] = v
		}
	}

	if v, ok := ma[mid]; !ok {
		summary.myCount = 0
	} else {
		summary.myCount = v.count
	}

	for _, j := range ma {
		mc = append(mc, j)
	}

	sort.Slice(mc, func(i, j int) bool {
		if v := mc[i].count - mc[j].count; v > 0 {
			return true
		} else if v > 0 {
			return false
		}
		if v := mc[i].data.Timestamp - mc[j].data.Timestamp; v < 0 {
			return true
		} else if v > 0 {
			return false
		}
		if mc[i].data.Issuer < mc[j].data.Issuer {
			return true
		} else {
			return false
		}
	})

	var q, o *FeData
	var qn, on int
	if len(mc) > fc.target && mc[0].count > fc.target {
		q  = mc[0].data
		qn = mc[0].count
	}
	if len(mc) > 0 && Id(mc[0].data) != mid {
		o  = mc[0].data
		on = mc[0].count
	}

	summary.target         = fc.target
	summary.nSent          = int(fc.nSent)
	summary.nReceived      = len(mp)
	summary.quorum         = q
	summary.quorumCount    = qn
	summary.candidate      = o
	summary.candidateCount = on

	if summary.quorum != nil || summary.nReceived >= summary.nSent {
		summary.done = true
	}

	{
		sb := &bytes.Buffer{}
		fmt.Fprintf(sb, "%d: %d/%d/%d: [", fc.Phase, summary.nSent,
			summary.target, summary.nReceived)
		space := ""
		for i, j := range ma {
			fmt.Fprintf(sb, "%s%s:%d", space, i, j.count)
			space = " "
		}
		fmt.Fprintf(sb, "] [")
		space = ""
		for i, j := range mp {
			fmt.Fprintf(sb, "%s%s:%s", space, i, j.Type)
			space = " "
		}
		fmt.Fprintf(sb, "]")

		fmt.Fprintf(sb, " [")
		space = ""
		for _, j := range fc.packets {
			if j.Type != "error" {
				fmt.Fprintf(sb, "%s%s:%s/%s/%d/%d/%d", space, j.From, j.Type,
					j.Issuer, j.Index, j.Phase, j.Timestamp)
			} else {
				fmt.Fprintf(sb, "%s%s:%s/%s/%s", space, j.From, j.Type,
					j.Key, j.Value)
			}
			space = " "
		}
		fmt.Fprintf(sb, "]")

		fmt.Fprintf(sb, " [")
		space = ""
		ct := time.Now().UnixNano()
		for _, i := range mc {
			j := i.data
			if j.Type != "error" {
				fmt.Fprintf(sb, "%s%s:%d/%s/%s/%d/%d/%d/%d", space, j.From,
					i.count, j.Type, j.Issuer, j.Index, j.Phase, j.Timestamp,
					ct - j.Timestamp)
			} else {
				fmt.Fprintf(sb, "%s%s:%s/%s/%s", space, j.From, j.Type,
					j.Key, j.Value)
			}
			space = " "
		}
		fmt.Fprintf(sb, "]")

		summary.summary = sb.String()
	}

	/* for yield, check count and hold-until */
	/* for commit, check index */

	//fmt.Printf("XXX: %+v\n", summary)
}

/* fc should be locked and state init */
func (fc *FeCollator) syncAndGo(nextTask *FeData) error {
	task := fc.prep("sync")
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

		fc.timeout(fc.Timeout, 0, "")
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
		q := fc.prep("get")
		q.Timestamp = time.Now().UnixNano()
		q.HoldUntil = req.Timestamp

		var ldata FeData
		ldata = *fc.data
		ldata.Type = "data"
		ldata.From = fc.me
		ldata.Phase = fc.Phase

		fc.timeout(fc.Timeout, 0, "")
		//atomic.AddInt32(&fc.nSent, len(fc.peers))
		fc.nSent = int32(len(fc.peers))
		fc.proc(&ldata, true)
		fc.sendToPeers(q)

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
			q := fc.prepError(data, 2)
			q.From = fc.me
			q.Value = "Index behind"
			return sendTo(data.From, q)
		} else if data.Index > fc.data.Index + 1 {
			// am i behind? sync and go, maybe?
			q := *data
			return fc.syncAndGo(&q)
		} else if data.HoldUntil <= ct {
			// it's expired already?
			q := fc.prepError(data, 3)
			q.From = fc.me
			q.Value = "Expired"
			return sendTo(data.From, q)
		} else {
			// we're good

			fc.setProposal(data)
			fc.setState("holding", true, true, false)

			q := fc.prep("holding")
			err := sendTo(data.From, q)
			if err == nil {
				fc.timeout(int((data.HoldUntil - ct) / 1000), 0, "")
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
		//fmt.Printf("Got obsolete: %+v\n", data)
	}
	return nil
}

func (fc *FeCollator) consumePackets(packets []*FeData) {
	for _, i := range packets {
		fc.proc(i, true)
	}
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

	case "hold": fallthrough
	case "cancel": fallthrough
	case "commit":
		fc.packets = append(fc.packets, data)
		return nil
	}

	var summary FeCollatorSummary
	fc.collate(&summary)
	if !done {
		done = summary.done
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
				q := fc.prepError(nil, 1)
				q.Value = "Quorum not reached"
				fc.task.tracker(q, "canceled", nil)
			}
			//fmt.Printf("Quorum not reached: %+v\n", summary)
		}

		packets := fc.packets
		fc.packets = nil
		if fc.task.next == nil {
			fc.putCaretaker(fc.task)
			fc.setState("init", true, true, true)
			fc.consumePackets(packets)
		} else {
			fc.putCaretaker(fc.task)
			task := fc.task.next
			fc.setState("init", true, true, false)
			err := fc.proc(task, true)
			fc.consumePackets(packets)
			return err
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

	timedout := false
	done := false
	switch data.Type {
	case "timeout":
		fmt.Printf("### timeout: proposing/%d/%d\n", fc.proposal.Index, fc.proposal.Phase)
		timedout = true
		done = true

	case "hold":
		q := fc.prep("holding")
		q.From = fc.me
		q.State = fc.proposal.State
		q.Phase = data.Phase
		err := sendTo(data.From, q)
		if err != nil {
			fmt.Printf("Failed to send to %s: %s\n", data.From, err)
		}

	case "error": fallthrough
	case "holding":
		fc.packets = append(fc.packets, data)

	case "yield":
		fmt.Printf("### got yield %s/%d/%d\n", data.From, data.Index, data.Phase)

		var summary FeCollatorSummary
		yield := false
		if data.Index == fc.proposal.Index {
			fc.collate(&summary)

			if summary.quorum == nil && summary.candidate != nil &&
				data.Issuer == summary.candidate.Issuer {
				yield = true
			}
		}

		if yield {
			fmt.Printf("### yielding to %s\n", data.Issuer)

			data.Type = "hold"
			fc.setProposal(data)
			fc.setState("holding", true, true, false)

			q := fc.prep("holding")
			err := sendTo(data.From, q)
			if err != nil {
				fmt.Printf("Failed to send holding to %s: %s\n",
					data.From, err)
			}

			q.Type = "yield"
			p := map[string]bool{}
			for _, i := range fc.packets {
				if _, ok := p[i.From]; ok {
					continue
				}
				if i.Type == "holding" && i.Issuer == fc.me &&
					i.Index == fc.proposal.Index &&
					i.Phase == fc.proposal.Phase {
					p[i.From] = true
				} else {
					continue
				}

				fmt.Printf("### sending yield to %s\n", i.From)
				err := sendTo(i.From, q)
				if err == nil {
					fmt.Printf("Failed to send yield to %s: %s\n",
						data.From, err)
				}
			}
		} else {
			// ignore
			fmt.Printf("%s: in %s, ignoring yield %+v, %+v: %s\n",
				fc.me, fc.State, data, fc.proposal, summary.summary)

			q := fc.prep("holding")
			q.From = fc.me
			q.Phase = data.Phase
			err := sendTo(data.From, q)
			if err != nil {
				fmt.Printf("Failed to send holding to %s: %s\n",
					data.From, err)
			}
		}

	case "commit":
		if data.Index < fc.proposal.Index {
			fmt.Printf("%s Got obsolete commit from %s for %d vs. %d\n",
				fc.me, data.From, data.Index, fc.proposal.Index)
		} else if data.Index >= fc.proposal.Index {
			fmt.Printf("%s Got commit from %s for %d vs. %d, sync and go.\n",
				fc.me, data.From, data.Index, fc.proposal.Index)

			q := fc.prep("cancel")
			q.From = fc.me
			fc.sendToPeers(q)

			task := fc.task
			fc.putCaretaker(fc.task)
			fc.setState("init", true, true, false)
			fc.syncAndGo(task)
			return nil
		}

	default:
		//fmt.Printf("%s XXX: %+v\n", fc.me, data)
		return nil
	}

	var summary FeCollatorSummary
	fc.collate(&summary)

	if done {
		//fmt.Println("###", summary.summary)
	}

	if !done {
		done = summary.done
	}

	if done {
		var sq, sc string
		if summary.quorum != nil {
			sq = summary.quorum.Issuer
		}
		if summary.candidate != nil {
			sc = summary.candidate.Issuer
		}

		fmt.Printf("### q=%s c=%s: %s\n", sq, sc, summary.summary)

		if summary.myCount < fc.target {
			if timedout {
				/* cancel them */
				q := fc.prep("cancel")
				q.From = fc.me
				fc.sendToPeers(q)

				if fc.task.tracker != nil {
					fc.task.tracker(summary.quorum, "canceled", nil)
				}

				fc.putCaretaker(fc.task)
				fc.setState("init", true, true, true)

				fmt.Printf("quorum not reached\n")
			} else if summary.needSync {
				fmt.Printf("### behind, need sync\n")

				/* sync and try again */
				q := fc.prep("cancel")
				q.From = fc.me
				fc.sendToPeers(q)

				if summary.quorum != nil {
					fmt.Printf("Other quorum, sync and go\n")
				} else {
					fmt.Printf("quorum not reached, sync and go\n")
				}

				task := fc.task
				fc.putCaretaker(fc.task)
				fc.setState("init", true, true, false)
				fc.syncAndGo(task)
			} else if summary.quorum == nil && summary.candidate == nil {
				// we are the one to move forward

				fmt.Println("### asking YIELD:", summary.summary)
				q := fc.prep("yield")
				q.From = fc.me

				var pkts []*FeData
				p := map[string]bool{}
				for _, i := range fc.packets {
					if i.Type != "holding" {
						pkts = append(pkts, i)
					} else {
						if i.From == fc.me || i.Issuer == fc.me {
							pkts = append(pkts, i)
						} else if i.From == i.Issuer {
							if _, ok := p[i.From]; ok {
								continue
							} else {
								p[i.From] = true
							}

							// send yield
							fmt.Printf("%s: in %s, sending yield to %s: %+v\n",
								fc.me, fc.State, i.From, q)
							err := sendTo(i.From, q)
							if err != nil {
								fmt.Printf("Failed to send yield to %s: %s\n",
									i.From, err)
							}
						} else {
							fmt.Printf("%s: dumping %s/%s\n", fc.me, i.From, i.Issuer)
						}
					}
				}

				fc.packets = pkts
				fmt.Printf("### repeat!\n")

				/* repeat */


			} else if true {
				/* sync and try again */
				q := fc.prep("cancel")
				q.From = fc.me
				fc.sendToPeers(q)

				if summary.quorum != nil {
					fmt.Printf("Other quorum, sync and go\n")
				} else {
					fmt.Printf("quorum not reached, sync and go\n")
				}

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
				q := fc.prep("cancel")
				q.From = fc.me
				fc.sendToPeers(q)

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

	case "error": fallthrough
	case "committed":
		fc.packets = append(fc.packets, data)

	default:
		//fmt.Printf("%s XXX: %+v\n", fc.me, data)
		return nil
	}

	var summary FeCollatorSummary
	fc.collate(&summary)
	if !done {
		done = summary.done
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
	if data.Channel != fc.Channel {
		return fmt.Errorf("wrong channel")
	}

	switch data.Type {
	case "hold":
		if data.From == data.Issuer && data.Issuer == fc.proposal.Issuer &&
			data.Index > fc.proposal.Index {
			// hold this one instead
			fc.setProposal(data)
			fc.setState("holding", true, true, false)
			q := fc.prep("holding")
			q.From = fc.me
			q.Phase = data.Phase
			_ = sendTo(data.From, q)
		} else if data.Issuer != fc.proposal.Issuer {
			// send "holding" message, keep it for yielding situation.
			fc.packets = append(fc.packets, data)
			q := fc.prep("holding")
			q.From = fc.me
			q.Phase = data.Phase
			_ = sendTo(data.From, q)
		} else {
			fmt.Printf("Got obsolete hold: hold=%+v, proposal=%+v\n",
				data, fc.proposal)
		}

	case "commit":
		if data.From == data.Issuer && data.Issuer == fc.proposal.Issuer &&
			data.Index == fc.proposal.Index {
			q := fc.prep("committed")
			q.From = fc.me
			err := sendTo(data.From, q)
			if err != nil {
				fmt.Printf("Failed to send to %s: %s\n", data.From, err)
			}

			x := fc.proposal
			fc.resetProposal()

			x.Type = "data"
			x.State = "committed"
			fc.setData(x)

			fc.setState("init", true, true, true)
		} else if data.Index > fc.proposal.Index {
			fmt.Printf("%s: in %s, got commit ahead, ignoring for now %+v, %+v\n",
				fc.me, fc.State, data, fc.proposal)
		} else {
			fmt.Printf("%s: in %s, ignoring %+v, %+v\n", fc.me, fc.State,
				data, fc.proposal)
		}

	case "cancel":
		if data.From == data.Issuer && data.Issuer == fc.proposal.Issuer &&
			data.Index == fc.proposal.Index {
			q := fc.prep("canceled")
			q.From = fc.me
			err := sendTo(data.From, q)
			if err != nil {
				fmt.Printf("Failed to send to %s: %s\n", data.From, err)
			}

			fc.resetProposal()
			fc.setState("init", true, true, true)
		} else {
			fmt.Printf("%s: in %s, ignoring %+v, %+v\n", fc.me, fc.State,
				data, fc.proposal)
		}

	case "yield":
		if data.From == fc.proposal.Issuer &&
			data.Index == fc.proposal.Index &&
			data.Issuer != fc.proposal.Issuer {
			// TODO: check data.Issuer is valid
			var prop *FeData
			for _, i := range fc.packets {
				if i.From == data.Issuer {
					prop = i
				}
			}

			if prop == nil {
				fmt.Printf("%s: in %s, got yield, doesn't have packet, canceling: %+v, %+v\n",
					fc.me, fc.State, data, fc.proposal)
				fc.resetProposal()
				fc.setState("init", true, true, true)
			} else {
				fc.setState("holding", true, true, true)
				fc.setProposal(data)

				q := fc.prep("holding")
				q.From = fc.me
				q.Phase = data.Phase
				err := sendTo(data.From, q)
				if err == nil {
					ct := time.Now().UnixNano()
					fc.timeout(int((data.HoldUntil - ct) / 1000), 0, "")
				}
				return err
			}
		} else {
			fmt.Printf("%s: in %s, ignoring yield %+v, %+v\n",
				fc.me, fc.State, data, fc.proposal)
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

	if data.Type == "put" && fc.State != "init" {
		panic(fmt.Sprintf("state=%s", fc.State))
	}

	if data.Type == "hold" {
		//fmt.Printf("### hold %s/%d/%d\n", data.From, data.Index, data.Phase)
	}
	if data.Type == "yield" {
		fmt.Printf("### yield %s/%d/%d\n", data.From, data.Index, data.Phase)
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
	fc.timeout(fc.Timeout, caretaker.Phase, "fin")
}

func (fc *FeCollator) putTask(task *FeData) error {
	fc.Tasks <- task
	go fc.pickup(false)
	return nil
}

/* EOF */
