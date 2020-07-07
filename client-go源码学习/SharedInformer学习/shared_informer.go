package cache

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/clock"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/buffer"

	"k8s.io/klog"
)

const (
	// syncedPollPeriod controls how often you look at the status of your sync funcs
	syncedPollPeriod = 100 * time.Millisecond
	// initialBufferSize is the initial number of event notifications that can be buffered.
	initialBufferSize = 1024
)

const minimumResyncPeriod = 1 * time.Second

type SharedInformer interface {
	// 添加资源事件处理器，关于ResourceEventHandler的定义在下面
	// 相当于注册回调函数，当有资源变化就会通过回调通知使用者，是不是能和上面介绍的Controller可以联系上了？
	// 为什么是Add不是Reg，说明可以支持多个handler
	AddEventHandler(handler ResourceEventHandler)
	// 上面添加的是不需要周期同步的处理器，下面的接口添加的是需要周期同步的处理器，周期同步上面提了好多遍了，不赘述
	AddEventHandlerWithResyncPeriod(handler ResourceEventHandler, resyncPeriod time.Duration)
	// Store这个有专门的文章介绍，这个函数就是获取Store的接口,说明SharedInformer内有Store对象
	GetStore() Store
	// Controller在上面的章节介绍了，说明SharedInformer内有Controller对象
	GetController() Controller
	// 这个应该是SharedInformer的核心逻辑实现的地方
	Run(stopCh <-chan struct{})
	// 因为有Store，这个函数就是告知使用者Store里面是否已经同步了apiserver的资源，这个接口很有用
	// 当创建完SharedInformer后，通过Reflector从apiserver同步全量对象，然后在通过DeltaFIFO一个一个的同志到cache
	// 这个接口就是告知使用者，全量的对象是不是已经同步到了cache，这样就可以从cache列举或者查询了
	HasSynced() bool
	// 最新同步资源的版本，这个就不多说了，通过Controller(Controller通过Reflector)实现
	LastSyncResourceVersion() string
}

type SharedIndexInformer interface {
	SharedInformer
	AddIndexers(indexers Indexers) error
	GetIndexer() Indexer
}

type InformerSynced func() bool

type updateNotification struct {
	oldObj interface{}
	newObj interface{}
}

type addNotification struct {
	newObj interface{}
}

type deleteNotification struct {
	oldObj interface{}
}

// lw:这个是api_server客户端相关的，用于Reflector从api_server获取资源，所以需要外部提供
// exampleObject:这个SharedInformer监控的对象类型
// defaultEventHandlerResyncPeriod:同步周期，SharedInformer需要多长时间给使用者发送一次全量对象的同步时间
func NewSharedInformer(lw ListerWatcher, exampleObject runtime.Object, defaultEventHandlerResyncPeriod time.Duration) SharedInformer {
	return NewSharedIndexInformer(lw, exampleObject, defaultEventHandlerResyncPeriod, Indexers{})
}

// 创建SharedIndexInformer对象，其中大部分参数再上面的函数已经介绍了
// indexers:需要外部提供计算对象索引键的函数，也就是这里面的对象需要通过什么方式创建索引
// 对象突变检测器删除，基本没有用，默认也不开启
func NewSharedIndexInformer(lw ListerWatcher, exampleObject runtime.Object, defaultEventHandlerResyncPeriod time.Duration, indexers Indexers) SharedIndexInformer {
	realClock := &clock.RealClock{}
	sharedIndexInformer := &sharedIndexInformer{
		// 管理所有处理器用的
		processor: &sharedProcessor{clock: realClock},
		// 其实就是在构造cache
		// 在cache中的对象用DeletionHandlingMetaNamespaceKeyFunc计算对象键，用indexers计算索引键
		// 可以想象成每个对象键是Namespace/Name，每个索引键是Namespace，即按照namespace分类
		// 因为objType决定了只有一种类型对象，所以namespace是最大的分类
		indexer: NewIndexer(DeletionHandlingMetaNamespaceKeyFunc, indexers),
		// 下面这两主要就是给Controller用，确切的说是给Reflector用的
		listerWatcher: lw,
		objectType:    exampleObject,
		// 无论是否需要定时同步，SharedInformer都提供了一个默认的同步时间，当然这个是外部设置的
		resyncCheckPeriod:               defaultEventHandlerResyncPeriod,
		defaultEventHandlerResyncPeriod: defaultEventHandlerResyncPeriod,
		// 默认没有开启的对象突变检测器
		clock:                 realClock,
	}
	return sharedIndexInformer
}

//1、利用api_server的api实现资源的列举和监控(Reflector实现)；
//2、利用cache存储api_server中的部分对象，通过对象类型进行制定，并在cache中采用Namespace做对象的索引
//3、先通过api_server的api将对象的全量列举出来存储在cache中，然后再watch资源，一旦有变化就更新cache中；
//4、更新到cache中的过程通过DeltaFIFO实现的有顺序的更新，因为资源状态是通过全量+增量方式实现同步的，所以顺序错误会造成状态不一致；
//5、使用者可以注册回调函数(类似挂钩子)，在更新到cache的同时通知使用者处理，为了保证回调处理不被某一个处理器阻塞，
//SharedInformer实现了processorListener异步缓冲处理；
type sharedIndexInformer struct {
	// Indexer也是一种Store，这个我们知道的，Controller负责把Reflector和FIFO逻辑串联起来
	// 所以这两个变量就涵盖了开篇那张图里面的Reflector、DeltaFIFO和LocalStore(cache)
	indexer    Indexer
	controller Controller
	// sharedIndexInformer把上面提到的ResourceEventHandler进行了在层封装，并统一由sharedProcessor管理
	processor *sharedProcessor
	// 这两个变量是给Reflector用的，我们知道Reflector是在Controller创建的
	listerWatcher ListerWatcher
	objectType    runtime.Object
	// 定期同步的周期，因为可能存在多个ResourceEventHandler，就有可能存在多个同步周期，sharedIndexInformer采用最小的周期
	// 这个周期值就存储在resyncCheckPeriod中，通过AddEventHandler()添加的处理器都采用defaultEventHandlerResyncPeriod
	resyncCheckPeriod               time.Duration
	defaultEventHandlerResyncPeriod time.Duration
	clock                           clock.Clock
	// 启动、停止标记，肯定有人会问为啥用两个变量，一个变量不就可以实现启动和停止了么？
	// 其实此处是三个状态，启动前，已启动和已停止，start表示了两个状态，而且为启动标记专门做了个锁
	// 说明启动前和启动后有互斥的资源操作
	started, stopped bool
	startedLock      sync.Mutex
	// 这个名字起的也是够了，因为DeltaFIFO每次Pop()的时候需要传入一个函数用来处理Deltas
	// 处理Deltas也就意味着要把消息通知给处理器，如果此时调用了AddEventHandler()
	// 就会存在崩溃的问题，所以要有这个锁，阻塞Deltas
	blockDeltas sync.Mutex
}

// sharedIndexInformer的核心逻辑函数
func (s *sharedIndexInformer) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	// 在此处构造的DeltaFIFO
	fifo := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KnownObjects:          s.indexer,
		EmitDeltaTypeReplaced: true,
	})
	// 这里的Config是我们介绍Reflector时介绍的那个Config
	cfg := &Config{
		Queue:            fifo,
		ListerWatcher:    s.listerWatcher,
		ObjectType:       s.objectType,
		FullResyncPeriod: s.resyncCheckPeriod,
		RetryOnError:     false,
		ShouldResync:     s.processor.shouldResync,
		// 这个是重点，Controller调用DeltaFIFO.Pop()接口传入的就是这个回调函数
		Process: s.HandleDeltas,
	}
	// 创建Controller，这个不用多说了
	// 通过这种方式实现defer调用unlock
	func() {
		s.startedLock.Lock()
		defer s.startedLock.Unlock()
		s.controller = New(cfg)
		s.controller.(*controller).clock = s.clock
		s.started = true
	}()
	// 这个processorStopCh 是给sharedProcessor和cacheMutationDetector传递退出信号的
	// 因为这里要创建两个协程运行sharedProcessor和cacheMutationDetector的核心函数
	processorStopCh := make(chan struct{})
	var wg wait.Group
	defer wg.Wait()
	defer close(processorStopCh)
	wg.StartWithChannel(processorStopCh, s.processor.run)
	// Run()函数都退出了，也就应该设置结束的标识了
	defer func() {
		s.startedLock.Lock()
		defer s.startedLock.Unlock()
		s.stopped = true
	}()
	// 启动Controller，Controller一旦运行，整个流程就开始启动了，所以叫Controller也不为过
	s.controller.Run(stopCh)
}

// 是否已经同步
func (s *sharedIndexInformer) HasSynced() bool {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()
	if s.controller == nil {
		return false
	}
	return s.controller.HasSynced()
}

// 最后同步的资源版本
func (s *sharedIndexInformer) LastSyncResourceVersion() string {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()
	if s.controller == nil {
		return ""
	}
	return s.controller.LastSyncResourceVersion()
}

func (s *sharedIndexInformer) GetStore() Store {
	return s.indexer
}

func (s *sharedIndexInformer) GetIndexer() Indexer {
	return s.indexer
}

func (s *sharedIndexInformer) AddIndexers(indexers Indexers) error {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()
	if s.started {
		return fmt.Errorf("informer has already started")
	}
	return s.indexer.AddIndexers(indexers)
}

func (s *sharedIndexInformer) GetController() Controller {
	return &dummyController{informer: s}
}

// 添加没有指定同步周期的事件处理器
func (s *sharedIndexInformer) AddEventHandler(handler ResourceEventHandler) {
	s.AddEventHandlerWithResyncPeriod(handler, s.defaultEventHandlerResyncPeriod)
}

// 添加需要定期同步的事件处理器
func (s *sharedIndexInformer) AddEventHandlerWithResyncPeriod(handler ResourceEventHandler, resyncPeriod time.Duration) {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()
	// 若informer已经关闭，则直接return
	if s.stopped {
		return
	}
	// resyncPeriod最小为minimumResyncPeriod
	if resyncPeriod > 0 {
		// 同步周期不能太短，太短对于系统来说反而是个负担，大量的无效计算浪费在这上面
		if resyncPeriod < minimumResyncPeriod {
			resyncPeriod = minimumResyncPeriod
		}
		// SharedInformer管理了很多处理器，每个处理器都有自己的同步周期，所以此处要统一成一个，称之为对齐
		// SharedInformer会选择所有处理器中最小的那个作为所有处理器的同步周期，称为对齐后的同步周期
		// 此处就要判断是不是比当前对齐后的同步周期还要小
		if resyncPeriod < s.resyncCheckPeriod {
			// 如果已经启动了，那么只能用和大家一样的周期
			if s.started {
				resyncPeriod = s.resyncCheckPeriod
			} else {
				// 如果没启动，那就让大家都用最新的对齐同步周期
				s.resyncCheckPeriod = resyncPeriod
				s.processor.resyncCheckPeriodChanged(resyncPeriod)
			}
		}
	}
	// 创建处理器，代码一直用listener,可能想强调没事件就挂起把，我反而想用处理器这个名词
	// determineResyncPeriod()这个函数读者自己分析把，非常简单，这里面只要知道创建了处理器就行了
	listener := newProcessListener(handler, resyncPeriod, determineResyncPeriod(resyncPeriod, s.resyncCheckPeriod), s.clock.Now(), initialBufferSize)
	// 如果没有启动，那么直接添加处理器就可以了
	if !s.started {
		s.processor.addListener(listener)
		return
	}
	// 这个锁就是暂停再想所有的处理器分发事件用的，因为这样会遍历所有的处理器，此时添加会有风险
	s.blockDeltas.Lock()
	defer s.blockDeltas.Unlock()
	// 添加处理器
	s.processor.addListener(listener)
	// 这里有意思啦，遍历缓冲中的所有对象，通知处理器，因为SharedInformer已经启动了，可能很多对象已经让其他的处理器处理过了，
	// 所以这些对象就不会再通知新添加的处理器，此处就是解决这个问题的
	for _, item := range s.indexer.List() {
		listener.add(addNotification{newObj: item})
	}
}

// obj处理函数
// 先处理indexer
// 再通知事件处理器
func (s *sharedIndexInformer) HandleDeltas(obj interface{}) error {
	s.blockDeltas.Lock()
	defer s.blockDeltas.Unlock()
	// Deltas里面包含了一个对象的多个增量操作，所以要从最老的Delta到最先的Delta遍历处理
	for _, d := range obj.(Deltas) {
		switch d.Type {
		case Sync, Replaced, Added, Updated:
			// 根据不同的Delta做不同的操作，但是大致分为对象添加、删除两大类操作
			// 所有的操作都要先同步到cache在通知处理器，这样保持处理器和cache的状态是一致的
			if old, exists, err := s.indexer.Get(d.Object); err == nil && exists {
				// 把对象更新到cache中
				if err := s.indexer.Update(d.Object); err != nil {
					return err
				}
				isSync := false
				switch {
				case d.Type == Sync:
					isSync = true
				case d.Type == Replaced:
					if accessor, err := meta.Accessor(d.Object); err == nil {
						if oldAccessor, err := meta.Accessor(old); err == nil {
							// Replaced events that didn't change resourceVersion are treated as resync events
							// and only propagated to listeners that requested resync
							isSync = accessor.GetResourceVersion() == oldAccessor.GetResourceVersion()
						}
					}
				}
				s.processor.distribute(updateNotification{oldObj: old, newObj: d.Object}, isSync)
			} else {
				// 把对象添加到cache中
				if err := s.indexer.Add(d.Object); err != nil {
					return err
				}
				// 通知处理器处理器事件
				s.processor.distribute(addNotification{newObj: d.Object}, false)
			}
		case Deleted:
			// 从cache删除一个obj
			if err := s.indexer.Delete(d.Object); err != nil {
				return err
			}
			// 通知事件监听器
			s.processor.distribute(deleteNotification{oldObj: d.Object}, false)
		}
	}
	return nil
}

// listener集合
type sharedProcessor struct {
	listenersStarted bool                 // 所有处理器是否已经启动的标识
	listenersLock    sync.RWMutex         // 读写锁
	listeners        []*processorListener // 通用的处理器
	syncingListeners []*processorListener // 需要定时同步的处理器
	clock            clock.Clock          // 时钟
	// 前面讲过了processorListener每个需要两个协程，
	// 用wait.Group来管理所有处理器的携程，保证他们都能退出
	wg wait.Group
}

// 添加一个listener
func (p *sharedProcessor) addListener(listener *processorListener) {
	p.listenersLock.Lock()
	defer p.listenersLock.Unlock()
	p.addListenerLocked(listener)
	// 通过wait.Group启动两个协程，做的事情我们在processorListener说过了，这里就是我们上面提到的启动两个协程的地方
	// 这个地方判断了listenersStarted，这说明sharedProcessor在启动前、后都可以添加处理器
	if p.listenersStarted {
		p.wg.Start(listener.run)
		p.wg.Start(listener.pop)
	}
}

func (p *sharedProcessor) addListenerLocked(listener *processorListener) {
	// 两类(定时同步和不同步)的处理器数组都添加了，这是因为没有定时同步的也会用默认的时间，后面我们会看到
	// 那么问题来了，那还用两个数组干什么呢？
	p.listeners = append(p.listeners, listener)
	p.syncingListeners = append(p.syncingListeners, listener)
}

// 分发事件
func (p *sharedProcessor) distribute(obj interface{}, sync bool) {
	p.listenersLock.RLock()
	defer p.listenersLock.RUnlock()
	// 无论是否为sync，添加处理器的代码中我们知道两个数组都会被添加，所以判断不判断没啥区别~
	// 所以我的猜测是代码以前实现的是明显区分两类的，但随着代码的更新二者的界限已经没那么明显了
	if sync {
		for _, listener := range p.syncingListeners {
			listener.add(obj)
		}
	} else {
		for _, listener := range p.listeners {
			listener.add(obj)
		}
	}
}

func (p *sharedProcessor) run(stopCh <-chan struct{}) {
	// 启动前、后对于添加处理器的逻辑是不同，启动前的处理器是不会立刻启动连个协程执行处理器的pop()和run()函数的
	// 而是在这里统一的启动
	func() {
		p.listenersLock.RLock()
		defer p.listenersLock.RUnlock()
		for _, listener := range p.listeners {
			p.wg.Start(listener.run)
			p.wg.Start(listener.pop)
		}
		p.listenersStarted = true
	}()
	<-stopCh
	// 关闭addCh，processorListener.pop()这个协程就会退出，不明白的可以再次回顾代码
	// 因为processorListener.pop()会关闭processorListener.nextCh，processorListener.run()就会退出
	// 所以这里只要关闭processorListener.addCh就可以自动实现两个协程的退出，不得不说设计的还是挺巧妙的
	for _, listener := range p.listeners {
		close(listener.addCh)
	}
	// 等待所有的协程退出，这里指的所有协程就是所有处理器的那两个协程
	p.wg.Wait()
}

// 是否需要同步
func (p *sharedProcessor) shouldResync() bool {
	p.listenersLock.Lock()
	defer p.listenersLock.Unlock()
	p.syncingListeners = []*processorListener{}
	resyncNeeded := false
	now := p.clock.Now()
	// 遍历所有的listeners查看是否需要同步，若需要同步则将listeners追加到p.syncingListeners里面
	for _, listener := range p.listeners {
		if listener.shouldResync(now) {
			resyncNeeded = true
			p.syncingListeners = append(p.syncingListeners, listener)
			listener.determineNextResync(now)
		}
	}
	return resyncNeeded
}

func (p *sharedProcessor) resyncCheckPeriodChanged(resyncCheckPeriod time.Duration) {
	p.listenersLock.RLock()
	defer p.listenersLock.RUnlock()
	for _, listener := range p.listeners {
		resyncPeriod := determineResyncPeriod(listener.requestedResyncPeriod, resyncCheckPeriod)
		listener.setResyncPeriod(resyncPeriod)
	}
}

// processor监听器
type processorListener struct {
	// nextCh、addCh、handler、pendingNotifications的用法请参看我的《golang的chan有趣用法》里面有相关的例子
	// 总结这四个变量实现了事件的输入、缓冲、处理，事件就是api_server资源的变化
	nextCh               chan interface{}
	addCh                chan interface{}
	handler              ResourceEventHandler //事件监听器
	pendingNotifications buffer.RingGrowing
	// 下面四个变量就是跟定时同步相关的了，requestedResyncPeriod是处理器设定的定时同步周期
	// resyncPeriod是跟sharedIndexInformer对齐的同步时间，因为sharedIndexInformer管理了多个处理器
	// 最终所有的处理器都会对齐到一个周期上，nextResync就是下一次同步的时间点
	requestedResyncPeriod time.Duration
	resyncPeriod          time.Duration
	nextResync            time.Time
	resyncLock            sync.Mutex
}

func newProcessListener(handler ResourceEventHandler, requestedResyncPeriod, resyncPeriod time.Duration, now time.Time, bufferSize int) *processorListener {
	ret := &processorListener{
		nextCh:                make(chan interface{}),
		addCh:                 make(chan interface{}),
		handler:               handler,
		pendingNotifications:  *buffer.NewRingGrowing(bufferSize),
		requestedResyncPeriod: requestedResyncPeriod,
		resyncPeriod:          resyncPeriod,
	}
	ret.determineNextResync(now)
	return ret
}

// 通过addCh传入，这里面的notification就是我们所谓的事件
func (p *processorListener) add(notification interface{}) {
	p.addCh <- notification
}

// 这个函数是通过sharedProcessor利用wait.Group启动的
func (p *processorListener) pop() {
	defer utilruntime.HandleCrash()
	// nextCh是在这里，函数退出前析构的
	defer close(p.nextCh)
	// 临时变量，下面会用到
	var nextCh chan<- interface{}
	var notification interface{}
	for {
		select {
		// 有两种情况，nextCh还没有初始化，这个语句就会被阻塞
		// nextChan后面会赋值为p.nextCh，因为p.nextCh也是无缓冲的chan，数据不发送成功就阻塞
		case nextCh <- notification:
			var ok bool
			// 如果发送成功了，那就从缓冲中再取一个事件出来
			notification, ok = p.pendingNotifications.ReadOne()
			if !ok {
				nextCh = nil
			}
		// 从p.addCh读取一个事件出来，这回看到消费p.addCh的地方了
		case notificationToAdd, ok := <-p.addCh:
			// 说明p.addCh关闭了，只能退出
			if !ok {
				return
			}
			// notification为空说明当前还没发送任何事件给处理器
			if notification == nil {
				// 那就把刚刚获取的事件通过p.nextCh发送个处理器
				notification = notificationToAdd
				nextCh = p.nextCh
			} else {
				// 上一个事件还没有发送成功，那就先放到缓存中
				// pendingNotifications可以想象为一个slice，这样方便理解，是一个动态的缓存，
				p.pendingNotifications.WriteOne(notificationToAdd)
			}
		}
	}
}

// 这个也是sharedProcessor通过wait.Group启动的
func (p *processorListener) run() {
	stopCh := make(chan struct{})
	wait.Until(func() {
		for next := range p.nextCh {
			switch notification := next.(type) {
			case updateNotification:
				p.handler.OnUpdate(notification.oldObj, notification.newObj)
			case addNotification:
				p.handler.OnAdd(notification.newObj)
			case deleteNotification:
				p.handler.OnDelete(notification.oldObj)
			default:
				utilruntime.HandleError(fmt.Errorf("unrecognized notification: %T", next))
			}
		}
		close(stopCh)
	}, 1*time.Second, stopCh)
}

// 是否需要同步，如果当前事件在p.nextResync之后或者相同，则返回true
func (p *processorListener) shouldResync(now time.Time) bool {
	p.resyncLock.Lock()
	defer p.resyncLock.Unlock()
	if p.resyncPeriod == 0 {
		return false
	}
	return now.After(p.nextResync) || now.Equal(p.nextResync)
}

// 获取下一次同步时间
func (p *processorListener) determineNextResync(now time.Time) {
	p.resyncLock.Lock()
	defer p.resyncLock.Unlock()
	p.nextResync = now.Add(p.resyncPeriod)
}

// 设置同步周期
func (p *processorListener) setResyncPeriod(resyncPeriod time.Duration) {
	p.resyncLock.Lock()
	defer p.resyncLock.Unlock()
	p.resyncPeriod = resyncPeriod
}

// 假的controller
type dummyController struct {
	informer *sharedIndexInformer
}

func (v *dummyController) Run(stopCh <-chan struct{}) {
}

func (v *dummyController) HasSynced() bool {
	return v.informer.HasSynced()
}

func (v *dummyController) LastSyncResourceVersion() string {
	return ""
}

func WaitForNamedCacheSync(controllerName string, stopCh <-chan struct{}, cacheSyncs ...InformerSynced) bool {
	if !WaitForCacheSync(stopCh, cacheSyncs...) {
		utilruntime.HandleError(fmt.Errorf("unable to sync caches for %s", controllerName))
		return false
	}
	return true
}

func WaitForCacheSync(stopCh <-chan struct{}, cacheSyncs ...InformerSynced) bool {
	err := wait.PollImmediateUntil(syncedPollPeriod,
		func() (bool, error) {
			for _, syncFunc := range cacheSyncs {
				if !syncFunc() {
					return false, nil
				}
			}
			return true, nil
		},
		stopCh)
	if err != nil {
		return false
	}
	return true
}

// 返回大的值
func determineResyncPeriod(desired, check time.Duration) time.Duration {
	if desired == 0 {
		return desired
	}
	if check == 0 {
		klog.Warningf("The specified resyncPeriod %v is invalid because this shared informer doesn't support resyncing", desired)
		return 0
	}
	if desired < check {
		klog.Warningf("The specified resyncPeriod %v is being increased to the minimum resyncCheckPeriod %v", desired, check)
		return check
	}
	return desired
}
