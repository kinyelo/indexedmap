package indexedmap

import (
	"runtime"
	"strconv"
	"strings"

	"github.com/puzpuzpuz/xsync"
)

// Index extraction function type
type IndexFunc[T any] func(obj *T) string

// Map data structure with seconday indexes.
// Stores pointers to elements of type T.
// Indexes are limited to string type only.
type IndexedMap[T any] struct {

	// Primary key index
	primary *xsync.Map

	// Secondary indexes by name.
	// Each secondary index is a map of index values and collections of objects
	// having this index value stored as map[K]*T
	secondary map[string]*xsync.Map

	// indexes configuration via map of index names and extraction functions
	indexes map[string]IndexFunc[T]
}

// Create new IndexedMap instance.
func NewIndexedMap[T any](indexes map[string]IndexFunc[T]) *IndexedMap[T] {
	r := IndexedMap[T]{
		primary:   xsync.NewMap(),
		secondary: map[string]*xsync.Map{},
		indexes:   indexes,
	}
	for name := range indexes {
		r.secondary[name] = xsync.NewMap()
	}
	return &r
}

// Add element to map using primary key of type int.
// Internally primary key is converted to string.
// This method has eventual consistency for primary and secondary indexes update.
// Primary index is updated after secondary.
func (r *IndexedMap[T]) PutInt(key int, obj T) {
	r.Put(strconv.Itoa(key), obj)
}

// Add element to map by primary key.
// This method has eventual consistency for primary and secondary indexes update.
// Primary index is updated after secondary.
func (r *IndexedMap[T]) Put(k string, obj T) {
	key := strings.ToUpper(k)
	if _, ok := r.Get(key); ok {
		for index := range r.indexes {
			r.updateIndex(index, &obj, key)
		}
	} else {
		for index := range r.indexes {
			r.putToIndex(index, strings.ToUpper(r.indexes[index](&obj)), &obj, key)
		}
	}
	r.primary.Store(key, obj)
}

// Put all array elements to indexed map. For arrays with more than 10k elements it works in parallel.
// keyFunc provides key extractor.
func (r *IndexedMap[T]) PutAll(arr []T, keyFunc func(*T) string) {
	count := len(arr)
	if count < 10000 {
		for _, t := range arr {
			r.Put(keyFunc(&t), t)
		}
		return
	}
	threads := runtime.NumCPU()
	batch := count / (threads - 1)
	ch := make(chan int, runtime.NumCPU())
	for i := range threads {
		go func() {
			for j := i * batch; j < (i+1)*batch; j++ {
				if j >= count {
					break
				}
				r.Put(keyFunc(&arr[j]), arr[j])
			}
			ch <- 1
		}()
	}
	waitChan(ch, threads)
	close(ch)
}

func waitChan(c chan int, num int) {
	for i := 0; i < num; i++ {
		<-c
	}
}

func (r *IndexedMap[T]) ContainsKeyInt(key int) bool {
	_, ok := r.GetInt(key)
	return ok
}

func (r *IndexedMap[T]) ContainsKey(key string) bool {
	_, ok := r.Get(key)
	return ok
}

// Get element from primary index by int key.
func (r *IndexedMap[T]) GetInt(key int) (T, bool) {
	return r.Get(strconv.Itoa(key))
}

// Get element from primary index.
func (r *IndexedMap[T]) Get(key string) (T, bool) {
	o, ok := r.primary.Load(strings.ToUpper(key))
	if ok {
		return o.(T), true
	}
	var zero T
	return zero, false
}

// Remove element from map by int primary key.
// This method has eventual consistency when secondary indexes are updated.
// There is a possibility that element will exist in primary index while partially removed from secondary indexes.
func (r *IndexedMap[T]) RemoveInt(key int) (T, bool) {
	return r.Remove(strconv.Itoa(key))
}

// Remove element from map by primary key.
// This method has eventual consistency when secondary indexes are updated.
// There is a possibility that element will exist in primary index while partially removed from secondary indexes.
func (r *IndexedMap[T]) Remove(k string) (T, bool) {
	key := strings.ToUpper(k)
	o, ok := r.Get(key)
	if ok {
		for name := range r.secondary {
			r.removeFromAllIndexLists(name, key)
		}
		r.primary.Delete(key)
		return o, true
	}
	var zero T
	return zero, false
}

func (r *IndexedMap[T]) removeFromAllIndexLists(name string, key string) {
	r.secondary[name].Range(func(k string, v any) bool {
		m := v.(*xsync.Map)
		m.Delete(key)
		return true
	})
}

// Get all keys from primary index.
func (r *IndexedMap[T]) Keys() []string {
	keys := make([]string, 0, r.Size())
	r.primary.Range(func(k string, b any) bool {
		keys = append(keys, k)
		return true
	})
	return keys
}

func (r *IndexedMap[T]) updateIndex(name string, obj *T, key string) {
	indexValue := strings.ToUpper(r.indexes[name](obj))
	prev, ok := r.Get(key)
	prevValue := ""
	if ok {
		prevValue = strings.ToUpper(r.indexes[name](&prev))
	}
	if indexValue == "" && prevValue == "" {
		return
	} else if indexValue != "" && indexValue == prevValue {
		r.putToIndex(name, indexValue, obj, key)
	} else if indexValue != "" && prevValue != "" && indexValue != prevValue {
		r.putToIndex(name, indexValue, obj, key)
		r.getIndexMapList(name, prevValue).Delete(key)
	} else if prevValue != "" && indexValue == "" {
		r.getIndexMapList(name, prevValue).Delete(key)
	}
}

func (r *IndexedMap[T]) getIndexMapList(name, indexValue string) *xsync.Map {
	v, ok := r.secondary[name].Load(indexValue)
	if !ok {
		v, _ = r.secondary[name].LoadOrStore(indexValue, xsync.NewMap())
	}
	return v.(*xsync.Map)
}

func (r *IndexedMap[T]) putToIndex(name string, indexValue string, obj *T, key string) {
	r.getIndexMapList(name, indexValue).Store(key, obj)
}

// Find all elements by index value.
func (r *IndexedMap[T]) GetByIndex(name string, v string) []T {
	indexValue := strings.ToUpper(v)
	result := []T{}
	r.getIndexMapList(name, indexValue).Range(func(k string, v any) bool {
		result = append(result, *v.(*T))
		return true
	})
	return result
}

// Get all values for specified index.
func (r *IndexedMap[T]) GetIndexKeys(name string) []string {
	result := []string{}
	r.secondary[name].Range(func(k string, v any) bool {
		result = append(result, k)
		return true
	})
	return result
}

// Get underlying sync.Map for selected index and value.
func (r *IndexedMap[T]) GetByIndexUnderlyingMap(name string, v string) *xsync.Map {
	indexValue := strings.ToUpper(v)
	return r.getIndexMapList(name, indexValue)
}

// Get underlying sync.Map for primary index.
func (r *IndexedMap[T]) GetPrimaryIndexUnderlyingMap() *xsync.Map {
	return r.primary
}

// Count elements in the indexed map.
func (r *IndexedMap[T]) Size() int {
	var i int
	r.primary.Range(func(k string, v any) bool {
		i++
		return true
	})
	return i
}
