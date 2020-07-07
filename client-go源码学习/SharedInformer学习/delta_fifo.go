package cache

import (
	"errors"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"

	"k8s.io/klog"
)

var (
	_ = Queue(&DeltaFIFO{})
)

var (
	ErrZeroLengthDeltasObject = errors.New("0 length Deltas object; can't get key")
)

// fifo关闭的异常
var ErrFIFOClosed = errors.New("DeltaFIFO: manipulating with closed queue")

type PopProcessFunc func(interface{}) error

type ErrRequeue struct {
	Err error
}

func (e ErrRequeue) Error() string {
	if e.Err == nil {
		return "the popped item should be requeued without returning an error"
	}
	return e.Err.Error()
}

type Queue interface {
	Store
	Pop(PopProcessFunc) (interface{}, error)
	AddIfNotPresent(interface{}) error
	HasSynced() bool
	Close()
}

type DeltaType string

const (
	Added    DeltaType = "Added"
	Updated  DeltaType = "Updated"
	Deleted  DeltaType = "Deleted"
	Replaced DeltaType = "Replaced"
	Sync     DeltaType = "Sync"
)

type Delta struct {
	Type   DeltaType
	Object interface{}
}

type Deltas []Delta

// 返回最早的Delta
func (d Deltas) Oldest() *Delta {
	if len(d) > 0 {
		return &d[0]
	}
	return nil
}

// 返回最新的Delta
func (d Deltas) Newest() *Delta {
	if n := len(d); n > 0 {
		return &d[n-1]
	}
	return nil
}

// 浅拷贝一个Deltas
func copyDeltas(d Deltas) Deltas {
	d2 := make(Deltas, len(d))
	copy(d2, d)
	return d2
}

type KeyListerGetter interface {
	KeyLister
	KeyGetter
}

type KeyLister interface {
	ListKeys() []string
}

type KeyGetter interface {
	GetByKey(key string) (interface{}, bool, error)
}

// ？？？？？？
type DeletedFinalStateUnknown struct {
	Key string
	Obj interface{}
}

func NewDeltaFIFO(keyFunc KeyFunc, knownObjects KeyListerGetter) *DeltaFIFO {
	return NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KeyFunction:  keyFunc,
		KnownObjects: knownObjects,
	})
}

type DeltaFIFOOptions struct {
	KeyFunction KeyFunc
	// 获取全量的数据
	KnownObjects KeyListerGetter
	// EmitDeltaTypeReplaced indicates that the queue consumer
	// understands the Replaced DeltaType. Before the `Replaced` event type was
	// added, calls to Replace() were handled the same as Sync(). For
	// backwards-compatibility purposes, this is false by default.
	// When true, `Replaced` events will be sent for items passed to a Replace() call.
	// When false, `Sync` events will be sent instead.
	EmitDeltaTypeReplaced bool
}

func NewDeltaFIFOWithOptions(opts DeltaFIFOOptions) *DeltaFIFO {
	if opts.KeyFunction == nil {
		opts.KeyFunction = MetaNamespaceKeyFunc
	}
	f := &DeltaFIFO{
		items:                 map[string]Deltas{},
		queue:                 []string{},
		keyFunc:               opts.KeyFunction,
		knownObjects:          opts.KnownObjects,
		emitDeltaTypeReplaced: opts.EmitDeltaTypeReplaced,
	}
	f.cond.L = &f.lock
	return f
}

type DeltaFIFO struct {
	lock                   sync.RWMutex      // 读写锁，因为涉及到同时读写，读写锁性能要高
	cond                   sync.Cond         // 给Pop()接口使用，在没有对象的时候可以阻塞，内部锁复用读写锁
	items                  map[string]Deltas // 这个应该是Store的本质了，按照kv的方式存储对象，但是存储的是对象的Deltas数组
	queue                  []string          // 这个是为先入先出实现的，存储的就是对象的键
	populated              bool              // 通过Replace()接口将第一批对象放入队列，或者第一次调用增、删、改接口时标记为true
	initialPopulationCount int               // 通过Replace()接口将第一批对象放入队列的对象数量
	keyFunc                KeyFunc           // 对象键计算函数，在Indexer那篇文章介绍过
	knownObjects           KeyListerGetter   // 该对象指向的就是Indexer
	closed                 bool              // 是否已经关闭的标记
	closedLock             sync.Mutex        // 专为关闭设计的所，为什么不复用读写锁？
	emitDeltaTypeReplaced  bool              // ？？？？？？
}

// 关闭队列
func (f *DeltaFIFO) Close() {
	f.closedLock.Lock()
	defer f.closedLock.Unlock()
	f.closed = true
	f.cond.Broadcast()
}

// 生成key
func (f *DeltaFIFO) KeyOf(obj interface{}) (string, error) {
	if d, ok := obj.(Deltas); ok {
		if len(d) == 0 {
			return "", KeyError{obj, ErrZeroLengthDeltasObject}
		}
		obj = d.Newest().Object
	}
	if d, ok := obj.(DeletedFinalStateUnknown); ok {
		return d.Key, nil
	}
	return f.keyFunc(obj)
}

// 是否已经同步了数据
func (f *DeltaFIFO) HasSynced() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.populated && f.initialPopulationCount == 0
}

// 增加一个obj
func (f *DeltaFIFO) Add(obj interface{}) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.populated = true
	return f.queueActionLocked(Added, obj)
}

// 更新一个obj
func (f *DeltaFIFO) Update(obj interface{}) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.populated = true
	return f.queueActionLocked(Updated, obj)
}

// 删除一个obj
// 需要校验存在性
func (f *DeltaFIFO) Delete(obj interface{}) error {
	id, err := f.KeyOf(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	f.populated = true
	if f.knownObjects == nil {
		if _, exists := f.items[id]; !exists {
			// 在没有Indexer的条件下只能通过自己存储的对象查一下
			return nil
		}
	} else {
		// 自己和Indexer里面有任何一个有这个对象都算存在
		_, exists, err := f.knownObjects.GetByKey(id)
		_, itemsExist := f.items[id]
		if err == nil && !exists && !itemsExist {
			return nil
		}
	}
	return f.queueActionLocked(Deleted, obj)
}

// 增加一个元素
// obj必须为Deltas类型
func (f *DeltaFIFO) AddIfNotPresent(obj interface{}) error {
	deltas, ok := obj.(Deltas)
	if !ok {
		return fmt.Errorf("object must be of type deltas, but got: %#v", obj)
	}
	id, err := f.KeyOf(deltas.Newest().Object)
	if err != nil {
		return KeyError{obj, err}
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	f.addIfNotPresent(id, deltas)
	return nil
}

// 插入一个Deltas，在pop process这个Deltas失败之后会调用此函数
func (f *DeltaFIFO) addIfNotPresent(id string, deltas Deltas) {
	f.populated = true
	if _, exists := f.items[id]; exists {
		return
	}
	f.queue = append(f.queue, id)
	f.items[id] = deltas
	f.cond.Broadcast()
}

// 真正的入队函数
func (f *DeltaFIFO) queueActionLocked(actionType DeltaType, obj interface{}) error {
	// 前面提到的计算对象键的函数
	id, err := f.KeyOf(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	// 追加Delta到f.items
	newDeltas := append(f.items[id], Delta{actionType, obj})
	// 是否可以合并
	newDeltas = dedupDeltas(newDeltas)
	// 若新的Deltas长度大于0，则替换原先的Deltas并进行广播
	if len(newDeltas) > 0 {
		if _, exists := f.items[id]; !exists {
			f.queue = append(f.queue, id)
		}
		f.items[id] = newDeltas
		f.cond.Broadcast()
	} else {
		// 直接把对象删除，这段代码我不知道什么条件会进来，因为dedupDeltas()肯定有返回结果的
		// 后面会有dedupDeltas()详细说明
		// ？？？？？？
		delete(f.items, id)
	}
	return nil
}

// 返回所有的最新的Delta
func (f *DeltaFIFO) List() []interface{} {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.listLocked()
}

func (f *DeltaFIFO) listLocked() []interface{} {
	list := make([]interface{}, 0, len(f.items))
	for _, item := range f.items {
		list = append(list, item.Newest().Object)
	}
	return list
}

// 返回所有的key
func (f *DeltaFIFO) ListKeys() []string {
	f.lock.RLock()
	defer f.lock.RUnlock()
	list := make([]string, 0, len(f.items))
	for key := range f.items {
		list = append(list, key)
	}
	return list
}

// 获取一个obj
func (f *DeltaFIFO) Get(obj interface{}) (item interface{}, exists bool, err error) {
	key, err := f.KeyOf(obj)
	if err != nil {
		return nil, false, KeyError{obj, err}
	}
	return f.GetByKey(key)
}

func (f *DeltaFIFO) GetByKey(key string) (item interface{}, exists bool, err error) {
	f.lock.RLock()
	defer f.lock.RUnlock()
	d, exists := f.items[key]
	if exists {
		d = copyDeltas(d)
	}
	return d, exists, nil
}

// 是否已经关闭
func (f *DeltaFIFO) IsClosed() bool {
	f.closedLock.Lock()
	defer f.closedLock.Unlock()
	return f.closed
}

// 弹出一个增量的obj
func (f *DeltaFIFO) Pop(process PopProcessFunc) (interface{}, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.lock.Lock()
	defer f.lock.Unlock()
	for {
		// 队列中有数据么？
		for len(f.queue) == 0 {
			// 看来是先判断的是否有数据，后判断是否关闭，这个和chan像
			if f.IsClosed() {
				return nil, ErrFIFOClosed
			}
			// 没数据那就等待把
			f.cond.Wait()
		}
		// 取出第一个对象
		id := f.queue[0]
		// 数组缩小，相当于把数组中的第一个元素弹出去了，这个不多解释哈
		f.queue = f.queue[1:]
		// 取出对象，因为queue中存的是对象键
		item, ok := f.items[id]
		// 同步对象计数减一，当减到0就说明外部已经全部同步完毕了
		if f.initialPopulationCount > 0 {
			f.initialPopulationCount--
		}
		// 对象不存在，这个是什么情况？貌似我们在合并对象的时候代码上有这个逻辑，估计永远不会执行
		if !ok {
			continue
		}
		// 把对象删除
		delete(f.items, id)
		// Pop()需要传入一个回调函数，用于处理对象
		err := process(item)
		// 如果需要重新入队列，那就重新入队列
		if e, ok := err.(ErrRequeue); ok {
			f.addIfNotPresent(id, item)
			err = e.Err
		}
		return item, err
	}
}

// 由于DeltaFIFO对外输出的就是所有目标的增量变化，所以每次全量更新都要判断对象是否已经删除，
// 因为在全量更新前可能没有收到目标删除的请求。
// 定期做全量更新调用此函数
func (f *DeltaFIFO) Replace(list []interface{}, resourceVersion string) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	keys := make(sets.String, len(list))
	// 设置action类型
	action := Sync
	if f.emitDeltaTypeReplaced {
		action = Replaced
	}
	// 遍历所有的输入目标
	for _, item := range list {
		key, err := f.KeyOf(item)
		if err != nil {
			return KeyError{item, err}
		}
		keys.Insert(key)
		// 将obj加入到queue和items
		if err := f.queueActionLocked(action, item); err != nil {
			return fmt.Errorf("couldn't enqueue object: %v", err)
		}
	}
	// 若存储为nil
	if f.knownObjects == nil {
		queuedDeletions := 0
		for k, oldItem := range f.items {
			if keys.Has(k) {
				continue
			}
			var deletedObj interface{}
			if n := oldItem.Newest(); n != nil {
				deletedObj = n.Object
			}
			queuedDeletions++
			if err := f.queueActionLocked(Deleted, DeletedFinalStateUnknown{k, deletedObj}); err != nil {
				return err
			}
		}
		if !f.populated {
			f.populated = true
			f.initialPopulationCount = len(list) + queuedDeletions
		}
		return nil
	}
	// 下面处理的就是检测某些目标删除但是Delta没有在队列中
	// 从存储中获取所有对象键
	knownKeys := f.knownObjects.ListKeys()
	queuedDeletions := 0
	for _, k := range knownKeys {
		if keys.Has(k) {
			continue
		}
		// 获取对象
		deletedObj, exists, err := f.knownObjects.GetByKey(k)
		if err != nil {
			deletedObj = nil
			klog.Errorf("Unexpected error %v during lookup of key %v, placing DeleteFinalStateUnknown marker without object", err, k)
		} else if !exists {
			deletedObj = nil
			klog.Infof("Key %v does not exist in known objects store, placing DeleteFinalStateUnknown marker without object", k)
		}
		// 累积删除的对象数量
		queuedDeletions++
		// 把对象删除的Delta放入队列
		if err := f.queueActionLocked(Deleted, DeletedFinalStateUnknown{k, deletedObj}); err != nil {
			return err
		}
	}
	if !f.populated {
		f.populated = true
		f.initialPopulationCount = len(list) + queuedDeletions
	}
	return nil
}

func (f *DeltaFIFO) Resync() error {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.knownObjects == nil {
		return nil
	}
	keys := f.knownObjects.ListKeys()
	for _, k := range keys {
		if err := f.syncKeyLocked(k); err != nil {
			return err
		}
	}
	return nil
}

// 具体对象同步实现接口
func (f *DeltaFIFO) syncKeyLocked(key string) error {
	obj, exists, err := f.knownObjects.GetByKey(key)
	if err != nil {
		klog.Errorf("Unexpected error %v during lookup of key %v, unable to queue object for sync", err, key)
		return nil
	} else if !exists {
		klog.Infof("Key %v does not exist in known objects store, unable to queue object for sync", key)
		return nil
	}
	// 计算对象的键值，有人会问对象键不是已经传入了么？那个是存在Indexer里面的对象键，可能与这里的计算方式不同
	id, err := f.KeyOf(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	// 对象已经在存在，说明后续会通知对象的新变化，所以再加更新也没意义
	if len(f.items[id]) > 0 {
		return nil
	}
	// 添加对象同步的这个Delta
	if err := f.queueActionLocked(Sync, obj); err != nil {
		return fmt.Errorf("couldn't queue object: %v", err)
	}
	return nil
}

// 1、DeltaFIFO生产者和消费者是异步的，如果同一个目标的频繁操作，前面操作还缓存在队列中的时候，
// 那么队列就要缓冲对象的所有操作，那可以将多个操作合并么？这是下面讨论的了；
// 2、对于更新这种类型的操作在没有全量基础的情况下是没法合并的，同时我们还不知道具体是什么类型的对象，
// 所以能合并的也就是有添加/删除，两个添加/删除操作其实可以视为一个；
// 合并Deltas
func dedupDeltas(deltas Deltas) Deltas {
	n := len(deltas)
	// 若小于2个，则无需合并
	if n < 2 {
		return deltas
	}
	// 取最后两个delta
	a := &deltas[n-1]
	b := &deltas[n-2]
	// 判断如果是重复的，那就删除这两个delta把合并后的追加到Deltas数组尾部
	if out := isDup(a, b); out != nil {
		// 删除最后两个元素
		d := append(Deltas{}, deltas[:n-2]...)
		// 合并的delta追加到
		return append(d, *out)
	}
	return deltas
}

// 判断两个delta是否可以合并
func isDup(a, b *Delta) *Delta {
	// 只有一个判断，只能判断是否为删除类操作，和我们上面的判断相同
	// 这个函数的本意应该还可以判断多种类型的重复，当前来看只能有删除这一种能够合并
	if out := isDeletionDup(a, b); out != nil {
		return out
	}
	return nil
}

// 判断是否为删除类的重复
func isDeletionDup(a, b *Delta) *Delta {
	// 二者都是删除那肯定有一个是重复的
	if b.Type != Deleted || a.Type != Deleted {
		return nil
	}
	// 理论上返回最后一个比较好，但是对象已经不在系统监控范围，前一个删除状态是好的
	if _, ok := b.Object.(DeletedFinalStateUnknown); ok {
		return a
	}
	return b
}
