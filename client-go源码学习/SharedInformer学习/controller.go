package cache

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/clock"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
)

type Controller interface {
	Run(stopCh <-chan struct{})
	HasSynced() bool
	LastSyncResourceVersion() string
}

type Config struct {
	Queue                             // SharedInformer使用DeltaFIFO
	ListerWatcher                     // 这个用来构造Reflector
	Process          ProcessFunc      // 这个在调用DeltaFIFO.Pop()使用，弹出对象要如何处理
	ObjectType       runtime.Object   // 对象类型，这个肯定是Reflector使用
	FullResyncPeriod time.Duration    // 全量同步周期，这个在Reflector使用
	ShouldResync     ShouldResyncFunc // Reflector在全量更新的时候会调用该函数询问
	RetryOnError     bool             // 错误是否需要尝试
}

// 是否需要同步
type ShouldResyncFunc func() bool

// 处理一个obj
type ProcessFunc func(obj interface{}) error

//Controller自己构造Reflector获取对象，Reflector作为DeltaFIFO生产者持续监控api_server的资源变化并推送到队列中。
//Controller的Run()应该是队列的消费者，从队列中弹出对象并调用Process()处理。
//所以Controller相比于Reflector因为队列的加持表现为每次有资源变化就会调用一次使用者定义的处理函数。
type controller struct {
	config         Config
	reflector      *Reflector
	reflectorMutex sync.RWMutex
	clock          clock.Clock
}

// 实例化一个controller
func New(c *Config) Controller {
	controller := &controller{
		config: *c,
		clock:  &clock.RealClock{},
	}
	return controller
}

// 主循环
func (c *controller) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	go func() {
		<-stopCh
		c.config.Queue.Close()
	}()
	r := NewReflector(
		c.config.ListerWatcher,
		c.config.ObjectType,
		c.config.Queue,
		c.config.FullResyncPeriod,
	)
	r.ShouldResync = c.config.ShouldResync
	r.clock = c.clock
	c.reflectorMutex.Lock()
	c.reflector = r
	c.reflectorMutex.Unlock()
	var wg wait.Group
	defer wg.Wait()
	// 开启reflector的主循环
	wg.StartWithChannel(stopCh, r.Run)
	// 开始process_loop
	wait.Until(c.processLoop, time.Second, stopCh)
}

// 如果已经完成了list的同步，则返回true
func (c *controller) HasSynced() bool {
	return c.config.Queue.HasSynced()
}

// 最后的同步的资源的版本
func (c *controller) LastSyncResourceVersion() string {
	c.reflectorMutex.RLock()
	defer c.reflectorMutex.RUnlock()
	if c.reflector == nil {
		return ""
	}
	return c.reflector.LastSyncResourceVersion()
}

// 主循环
func (c *controller) processLoop() {
	for {
		obj, err := c.config.Queue.Pop(PopProcessFunc(c.config.Process))
		if err != nil {
			if err == ErrFIFOClosed {
				return
			}
			if c.config.RetryOnError {
				_ = c.config.Queue.AddIfNotPresent(obj)
			}
		}
	}
}

// 事件处理器
type ResourceEventHandler interface {
	OnAdd(obj interface{})
	OnUpdate(oldObj, newObj interface{})
	OnDelete(obj interface{})
}

type ResourceEventHandlerFunc struct {
	AddFunc    func(obj interface{})
	UpdateFunc func(oldObj, newObj interface{})
	DeleteFunc func(obj interface{})
}

func (r ResourceEventHandlerFunc) OnAdd(obj interface{}) {
	if r.AddFunc != nil {
		r.AddFunc(obj)
	}
}

func (r ResourceEventHandlerFunc) OnUpdate(oldObj, newObj interface{}) {
	if r.UpdateFunc != nil {
		r.UpdateFunc(oldObj, newObj)
	}
}

func (r ResourceEventHandlerFunc) OnDelete(obj interface{}) {
	if r.DeleteFunc != nil {
		r.DeleteFunc(obj)
	}
}

type FilteringResourceEventHandler struct {
	FilterFunc func(obj interface{}) bool
	Handler    ResourceEventHandler
}

func (r FilteringResourceEventHandler) OnAdd(obj interface{}) {
	if !r.FilterFunc(obj) {
		return
	}
	r.Handler.OnAdd(obj)
}

func (r FilteringResourceEventHandler) OnUpdate(oldObj, newObj interface{}) {
	newer := r.FilterFunc(newObj)
	older := r.FilterFunc(oldObj)
	switch {
	case newer && older:
		r.Handler.OnUpdate(oldObj, newObj)
	case newer && !older:
		r.Handler.OnAdd(newObj)
	case !newer && older:
		r.Handler.OnDelete(oldObj)
	default:
	}
}

func (r FilteringResourceEventHandler) OnDelete(obj interface{}) {
	if !r.FilterFunc(obj) {
		return
	}
	r.Handler.OnDelete(obj)
}

// 实例化一个informer
func NewInformer(
	lw ListerWatcher,
	objType runtime.Object,
	resyncPeriod time.Duration,
	h ResourceEventHandler,
) (Store, Controller) {
	clientState := NewStore(DeletionHandlingMetaNamespaceKeyFunc)
	return clientState, newInformer(lw, objType, resyncPeriod, h, clientState)
}

// 实例化一个带索引的informer
func NewIndexerInformer(
	lw ListerWatcher,
	objType runtime.Object,
	resyncPeriod time.Duration,
	h ResourceEventHandler,
	indexers Indexers,
) (Indexer, Controller) {
	clientState := NewIndexer(DeletionHandlingMetaNamespaceKeyFunc, indexers)
	return clientState, newInformer(lw, objType, resyncPeriod, h, clientState)
}

// 真正实例化informer的函数
func newInformer(
	lw ListerWatcher,
	objType runtime.Object,
	resyncPeriod time.Duration,
	h ResourceEventHandler,
	clientState Store,
) Controller {
	fifo := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KnownObjects:          clientState,
		EmitDeltaTypeReplaced: true,
	})
	cfg := &Config{
		Queue:            fifo,
		ListerWatcher:    lw,
		ObjectType:       objType,
		FullResyncPeriod: resyncPeriod,
		RetryOnError:     false,
		Process: func(obj interface{}) error {
			for _, d := range obj.(Deltas) {
				switch d.Type {
				case Sync, Replaced, Added, Updated:
					if old, exists, err := clientState.Get(d.Object); err == nil && exists {
						if err := clientState.Update(d.Object); err != nil {
							return err
						}
						h.OnUpdate(old, d.Object)
					} else {
						if err := clientState.Add(d.Object); err != nil {
							return err
						}
						h.OnAdd(d.Object)
					}
				case Deleted:
					if err := clientState.Delete(d.Object); err != nil {
						return err
					}
					h.OnDelete(d.Object)
				}
			}
			return nil
		},
	}
	return New(cfg)
}

// key func
func DeletionHandlingMetaNamespaceKeyFunc(obj interface{}) (string, error) {
	if d, ok := obj.(DeletedFinalStateUnknown); ok {
		return d.Key, nil
	}
	return MetaNamespaceKeyFunc(obj)
}
