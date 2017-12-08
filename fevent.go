/* fevent.go */

package fe

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
)

var (
	_id uint64 = 0
	_registry = &sync.Map{}
)

func FeventGetId() uint64 {
	return atomic.AddUint64(&_id, 1)
}

func FeventGetStringId() string {
	return strconv.FormatUint(FeventGetId(), 10)
}

func FeventRegister(id interface{}, fn func(param, data interface{})error, param interface{}) {
	var x []interface{}
	x = append(append(x, fn), param)
	_registry.Store(id, x)
	//fmt.Printf("Event %s registered\n", id.(string))
}

func FeventUnregister(id interface{}) {
	_registry.Delete(id)
	//fmt.Printf("Event %s unregistered\n", id.(string))
}

func FeventProcess(id, data interface{}) error {
	_x, ok := _registry.Load(id)
	if !ok {
		return fmt.Errorf("No registered event found for %s!", id.(string))
	} else {
		x := _x.([]interface{})
		f := x[0].(func(interface{}, interface{})error)
		return f(x[1], data)
	}
}

func FeventProcessData(data *FeData) error {
	return FeventProcess(data.Channel, data)
}

/* EOF */
