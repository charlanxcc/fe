/* fe-memdb.go */

package fe

import (
	_ "fmt"
	_ "strconv"
	"sync"
	_ "time"
)

var (
	feDb = &sync.Map{}
	feHashMask = 0x3ff
	feHashLock = make([]sync.RWMutex, feHashMask + 1,  feHashMask + 1)

	feLock = &sync.RWMutex{}
)

/*
func (m *Map) Delete(key interface{})
func (m *Map) Load(key interface{}) (value interface{}, ok bool)
func (m *Map) LoadOrStore(key, value interface{}) (actual interface{}, loaded bool)
func (m *Map) Range(f func(key, value interface{}) bool)
func (m *Map) Store(key, value interface{})
*/

/*
func getCurrent(data *FeData) {
	_data, ok := feDb.Load("current")
	if ok {
		*data = _data.(FeData)
	} else {
		*data = *feDataGenesis
		feDb.Store("current", *data)
		feDb.Store(strconv.FormatUint(data.Index, 10), *data)
	}
}

func GetCurrent(data *FeData) {
	feLock.RLock()
	defer feLock.RUnlock()
	getCurrent(data)
}

func setCurrent(data *FeData) {
	feDb.Store(strconv.FormatUint(data.Index, 10), *data)
	feDb.Store("current", *data)
}

func SetCurrent(data *FeData) {
	feLock.Lock()
	defer feLock.Unlock()
	setCurrent(data)
}

func getProposal(_data string, holdDuration int, data *FeData) (ok, new bool) {
	var ct = time.Now().UnixNano()

	_prop, ok := feDb.Load("proposal")
	if ok {
		*data = _prop.(FeData)
		if data.HoldUntil < ct {
			feDb.Delete("proposal")
		} else {
			return true, false
		}
	}

	if _data == "" {
		return false, false
	}

	var current FeData
	getCurrent(&current)

	*data = FeData {
		Owner: me.Id,
		Index: current.Index + 1,
		Data: _data,
		Timestamp: ct,
		State: "proposing",
		HoldUntil: ct + (time.Duration(holdDuration) * time.Second / time.Nanosecond).Nanoseconds(),
	}
	feDb.Store("proposal", *data)
	return true, true
}

func GetProposal(_data string, holdDuration int, data *FeData) (ok, new bool) {
	feLock.Lock()
	defer feLock.Unlock()
	return getProposal(_data, holdDuration, data)
}

func setProposal(p *FeData) error {
	var op FeData
	ok, _ := getProposal("", 0, &op)
	if ok {
		return fmt.Errorf("in progress")
	}

	feDb.Store("proposal", *p)
	return nil
}

func SetProposal(p *FeData) error {
	feLock.Lock()
	defer feLock.Unlock()
	return setProposal(p)
}

func UpdateProposal(p *FeData) error {
	feLock.Lock()
	defer feLock.Unlock()

	var op FeData
	ok, _ := getProposal("", 0, &op)
	if !ok || op.Cmp(p, false) != 0 {
		return fmt.Errorf("different proposals")
	} else if p.State != op.State {
		setProposal(p)
	}
	return nil
}

func CommitProposal(p *FeData) error {
	feLock.Lock()
	defer feLock.Unlock()
	
	var op FeData
	ok, _ := getProposal("", 0, &op)
	if !ok || op.Cmp(p, false) != 0 {
		return fmt.Errorf("different proposals")
	} else {
		setCurrent(p)
		feDb.Delete("proposal")
	}
	return nil
}

func UnsetProposal(p *FeData) {
	feLock.Lock()
	defer feLock.Unlock()

	var op FeData
	ok, _ := getProposal("", 0, &op)
	if ok && op.Cmp(p, false) == 0 {
		feDb.Delete("proposal")
	}
}
*/

/* EOF */
