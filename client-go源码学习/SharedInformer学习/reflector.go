package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/apimachinery/pkg/util/naming"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/pager"
	"k8s.io/klog"
)

var (
	// nothing will ever be sent down this channel
	neverExitWatch <-chan time.Time = make(chan time.Time)
	// Used to indicate that watching stopped because of a signal from the stop
	// channel passed in from a client of the reflector.
	errorStopRequested = errors.New("stop requested")
)

var (
	// We try to spread the load on apiserver by setting timeouts for
	// watch requests - it is random in [minWatchTimeout, 2*minWatchTimeout].
	minWatchTimeout = 5 * time.Minute
)

// internalPackages are packages that ignored when creating a default reflector name. These packages are in the common
// call chains to NewReflector, so they'd be low entropy names for reflectors
var internalPackages = []string{"client-go/tools/cache/"}

const defaultExpectedTypeName = "<unspecified>"

// listerWatcher用于获取和监控资源，lister可以获取对象的全量，watcher可以获取对象的增量(变化)；
// 系统会周期性的执行list-watch的流程，一旦过程中失败就要重新执行流程，这个重新执行的周期就是period指定的；
// expectedType规定了监控对象的类型，非此类型的对象将会被忽略；
// 实例化后的expectedType类型的对象会被添加到store中；
// kubernetes资源在api_server中都是有版本的，对象的任何除了修改(添加、删除、更新)都会造成资源版本更新，所以lastSyncResourceVersion就是指的这个版本；
// 如果使用者需要定期同步全量对象，那么Reflector就会定期产生全量对象的同步事件给DeltaFIFO;
type Reflector struct {
	// 名字
	name string
	// 反射的类型名称
	expectedTypeName string
	// 反射的类型，也就是要监控的对象类型，比如Pod
	expectedType reflect.Type
	// The GVK of the object we expect to place in the store if unstructured.
	// ？？？？？？
	expectedGVK *schema.GroupVersionKind
	// 存储，就是DeltaFIFO
	store Store
	// 这个是用来从api_server获取资源用的
	listerWatcher ListerWatcher
	// 反射器在List和Watch的时候理论上是死循环，只有出现错误才会退出
	// 这个变量用在出错后多长时间再执行List和Watch，默认值是1秒钟
	backoffManager wait.BackoffManager
	// 重新同步的周期，很多人肯定认为这个同步周期指的是从api_server的同步周期
	// 其实这里面同步指的是shared_informer使用者需要定期同步全量对象
	resyncPeriod time.Duration
	// 如果需要同步，调用这个函数，当然前提是该函数指针不为空
	ShouldResync    func() bool
	clock           clock.Clock
	paginatedResult bool
	// 最后一次同步的资源版本
	lastSyncResourceVersion string
	// 同步的时候发生了410错误码
	isLastSyncResourceVersionGone bool
	// 还专门为最后一次同步的资源版本弄了个锁
	lastSyncResourceVersionMutex sync.RWMutex
	// WatchListPageSize is the requested chunk size of initial and resync watch lists.
	// If unset, for consistent reads (RV="") or reads that opt-into arbitrarily old data
	// (RV="0") it will default to pager.PageSize, for the rest (RV != "" && RV != "0")
	// it will turn off pagination to allow serving them from watch cache.
	// NOTE: It should be used carefully as paginated lists are always served directly from
	// etcd, which is significantly less efficient and may lead to serious performance and
	// scalability problems.
	WatchListPageSize int64
}

func NewNamespaceKeyedIndexerAndReflector(lw ListerWatcher, expectedType interface{}, resyncPeriod time.Duration) (indexer Indexer, reflector *Reflector) {
	indexer = NewIndexer(MetaNamespaceKeyFunc, Indexers{NamespaceIndex: MetaNamespaceIndexFunc})
	reflector = NewReflector(lw, expectedType, indexer, resyncPeriod)
	return indexer, reflector
}

func NewReflector(lw ListerWatcher, expectedType interface{}, store Store, resyncPeriod time.Duration) *Reflector {
	return NewNamedReflector(naming.GetNameFromCallsite(internalPackages...), lw, expectedType, store, resyncPeriod)
}

func NewNamedReflector(name string, lw ListerWatcher, expectedType interface{}, store Store, resyncPeriod time.Duration) *Reflector {
	realClock := &clock.RealClock{}
	r := &Reflector{
		name:          name,
		listerWatcher: lw,
		store:         store,
		// We used to make the call every 1sec (1 QPS), the goal here is to achieve ~98% traffic reduction when
		// API server is not healthy. With these parameters, backoff will stop at [30,60) sec interval which is
		// 0.22 QPS. If we don't backoff for 2min, assume API server is healthy and we reset the backoff.
		backoffManager: wait.NewExponentialBackoffManager(800*time.Millisecond, 30*time.Second, 2*time.Minute, 2.0, 1.0, realClock),
		resyncPeriod:   resyncPeriod,
		clock:          realClock,
	}
	r.setExpectedType(expectedType)
	return r
}

// 定期调用ListAndWatch方法
// 这里面我们不用关心wait.BackoffUntil是如何实现的，只要知道他调用函数f会被backoffManager周期执行一次
func (r *Reflector) Run(stopCh <-chan struct{}) {
	wait.BackoffUntil(func() {
		if err := r.ListAndWatch(stopCh); err != nil {
			utilruntime.HandleError(err)
		}
	}, r.backoffManager, true, stopCh)
}

// list and watch函数
func (r *Reflector) ListAndWatch(stopCh <-chan struct{}) error {
	// 很多存储类的系统都是这样设计的，数据采用版本的方式记录，数据每变化(添加、删除、更新)都会触发版本更新，
	// 这样的做法可以避免全量数据访问。以api_server资源监控为例，只要监控比缓存中资源版本大的对象就可以了，
	// 把变化的部分更新到缓存中就可以达到与api_server一致的效果，一般资源的初始版本为0，从0版本开始列举就是全量的对象了
	var resourceVersion string
	options := metav1.ListOptions{ResourceVersion: r.relistResourceVersion()}
	// 1、通过list获取资源
	if err := func() error {
		var list runtime.Object
		var paginatedResult bool
		var err error
		listCh := make(chan struct{}, 1)
		panicCh := make(chan interface{}, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					panicCh <- r
				}
			}()
			p := pager.New(pager.SimplePageFunc(func(opts metav1.ListOptions) (runtime.Object, error) {
				return r.listerWatcher.List(opts)
			}))
			switch {
			case r.WatchListPageSize != 0:
				p.PageSize = r.WatchListPageSize
			case r.paginatedResult:
			case options.ResourceVersion != "" && options.ResourceVersion != "0":
				p.PageSize = 0
			}
			list, paginatedResult, err = p.List(context.Background(), options)
			if isExpiredError(err) {
				r.setIsLastSyncResourceVersionExpired(true)
				list, paginatedResult, err = p.List(context.Background(), metav1.ListOptions{ResourceVersion: r.relistResourceVersion()})
			}
			close(listCh)
		}()
		select {
		case <-stopCh:
			return nil
		case r := <-panicCh:
			panic(r)
		case <-listCh:
		}
		if err != nil {
			return fmt.Errorf("%s: Failed to list %v: %v", r.name, r.expectedTypeName, err)
		}
		if options.ResourceVersion == "0" && paginatedResult {
			r.paginatedResult = true
		}
		r.setIsLastSyncResourceVersionExpired(false) // list was successful
		listMetaInterface, err := meta.ListAccessor(list)
		if err != nil {
			return fmt.Errorf("%s: Unable to understand list result %#v: %v", r.name, list, err)
		}
		resourceVersion = listMetaInterface.GetResourceVersion()
		items, err := meta.ExtractList(list)
		if err != nil {
			return fmt.Errorf("%s: Unable to understand list result %#v (%v)", r.name, list, err)
		}
		if err := r.syncWith(items, resourceVersion); err != nil {
			return fmt.Errorf("%s: Unable to sync list result: %v", r.name, err)
		}
		r.setLastSyncResourceVersion(resourceVersion)
		return nil
	}(); err != nil {
		return err
	}
	// 2、下面要启动一个后台协程实现定期的同步操作，这个同步就是将SharedInformer里面的对象全量以同步事件的方式通知使用者
	// 我们暂且称之为“后台同步协程”，Run()函数退出需要后台同步协程退出，所以下面的cancelCh就是干这个用的
	// 利用defer close(cancelCh)实现的，而resyncErrC是后台同步协程反向通知Run()函数的报错通道
	// 当后台同步协程出错，Run()函数接收到信号就可以退出了
	resyncErrC := make(chan error, 1)
	cancelCh := make(chan struct{})
	defer close(cancelCh)
	go func() {
		resyncCh, cleanup := r.resyncChan()
		defer func() {
			cleanup()
		}()
		for {
			select {
			case <-resyncCh:
			case <-stopCh:
				return
			case <-cancelCh:
				return
			}
			if r.ShouldResync == nil || r.ShouldResync() {
				klog.V(4).Infof("%s: forcing resync", r.name)
				if err := r.store.Resync(); err != nil {
					resyncErrC <- err
					return
				}
			}
			cleanup()
			resyncCh, cleanup = r.resyncChan()
		}
	}()
	// 3、前面已经列举了全量对象，接下来就是watch的逻辑了
	for {
		// 如果有退出信号就立刻返回，否则就会往下走，因为有default
		select {
		case <-stopCh:
			return nil
		default:
		}
		// 计算watch的超时时间
		timeoutSeconds := int64(minWatchTimeout.Seconds() * (rand.Float64() + 1.0))
		// 设置watch的选项，因为前期列举了全量对象，从这里只要监听最新版本以后的资源就可以了
		// 如果没有资源变化总不能一直挂着吧？也不知道是卡死了还是怎么了，所以有一个超时会好一点
		options = metav1.ListOptions{
			ResourceVersion:     resourceVersion,
			TimeoutSeconds:      &timeoutSeconds,
			AllowWatchBookmarks: true,
		}
		start := r.clock.Now()
		// 开始监控对象
		w, err := r.listerWatcher.Watch(options)
		// watch产生错误了，大部分错误就要退出函数然后再重新来一遍流程
		if err != nil {
			switch {
			case isExpiredError(err):
				// Don't set LastSyncResourceVersionExpired - LIST call with ResourceVersion=RV already
				// has a semantic that it returns data at least as fresh as provided RV.
				// So first try to LIST with setting RV to resource version of last observed object.
				klog.V(4).Infof("%s: watch of %v closed with: %v", r.name, r.expectedTypeName, err)
			case err == io.EOF:
			case err == io.ErrUnexpectedEOF:
				klog.V(1).Infof("%s: Watch for %v closed with unexpected EOF: %v", r.name, r.expectedTypeName, err)
			default:
				utilruntime.HandleError(fmt.Errorf("%s: Failed to watch %v: %v", r.name, r.expectedTypeName, err))
			}
			// 类似于网络拒绝连接的错误要等一会儿再试，因为可能网络繁忙
			// 这种时候不需要重新list资源，重启watch资源即可
			if utilnet.IsConnectionRefused(err) {
				time.Sleep(time.Second)
				continue
			}
			return nil
		}
		// watch返回是流，api_server会将变化的资源通过这个流发送出来，client-go最终通过chan实现的
		// 所以watchHandler()是一个需要持续从chan读取数据的流程，所以需要传入resyncErrC和stopCh
		// 用于异步通知退出或者后台同步协程错误
		if err := r.watchHandler(start, w, &resourceVersion, resyncErrC, stopCh); err != nil {
			if err != errorStopRequested {
				switch {
				case isExpiredError(err):
					// Don't set LastSyncResourceVersionExpired - LIST call with ResourceVersion=RV already
					// has a semantic that it returns data at least as fresh as provided RV.
					// So first try to LIST with setting RV to resource version of last observed object.
					klog.V(4).Infof("%s: watch of %v closed with: %v", r.name, r.expectedTypeName, err)
				default:
					klog.Warningf("%s: watch of %v ended with: %v", r.name, r.expectedTypeName, err)
				}
			}
			return nil
		}
	}
}

// LastSyncResourceVersion is the resource version observed when last sync with the underlying store
// The value returned is not synchronized with access to the underlying store and is not thread-safe
func (r *Reflector) LastSyncResourceVersion() string {
	r.lastSyncResourceVersionMutex.RLock()
	defer r.lastSyncResourceVersionMutex.RUnlock()
	return r.lastSyncResourceVersion
}

// 定时通知Informer使用者同步数据
func (r *Reflector) resyncChan() (<-chan time.Time, func() bool) {
	if r.resyncPeriod == 0 {
		return neverExitWatch, func() bool { return false }
	}
	t := r.clock.NewTimer(r.resyncPeriod)
	return t.C(), t.Stop
}

// 实现api_server全量对象的同步
// 将数据传给delta_fifo队列
func (r *Reflector) syncWith(items []runtime.Object, resourceVersion string) error {
	found := make([]interface{}, 0, len(items))
	for _, item := range items {
		found = append(found, item)
	}
	return r.store.Replace(found, resourceVersion)
}

// 监听w
func (r *Reflector) watchHandler(start time.Time, w watch.Interface, resourceVersion *string, errC chan error, stopCh <-chan struct{}) error {
	eventCount := 0
	// 有异常退出需要关闭w
	defer w.Stop()
	// 这里就开始无限循环的从chan中读取资源的变化，也可以理解为资源的增量变化，同时还要监控各种信号
loop:
	for {
		select {
		case <-stopCh:
			return errorStopRequested
		case err := <-errC:
			return err
		case event, ok := <-w.ResultChan():
			// 如果w返回ok为false，则break loop
			if !ok {
				break loop
			}
			// 看来event可以作为错误的返回值，挺有意思，而不是通过关闭chan，这种方式可以传递错误信息，关闭chan做不到
			if event.Type == watch.Error {
				return apierrors.FromObject(event.Object)
			}
			if r.expectedType != nil {
				// 不是预期类型则跳过
				if e, a := r.expectedType, reflect.TypeOf(event.Object); e != a {
					utilruntime.HandleError(fmt.Errorf("%s: expected type %v, but watch event object had type %v", r.name, e, a))
					continue
				}
			}
			// gvk不清楚原因
			// ？？？？？？
			if r.expectedGVK != nil {
				if e, a := *r.expectedGVK, event.Object.GetObjectKind().GroupVersionKind(); e != a {
					utilruntime.HandleError(fmt.Errorf("%s: expected gvk %v, but watch event object had gvk %v", r.name, e, a))
					continue
				}
			}
			// 和list操作相似，也要获取对象的版本，要更新缓存中的版本，下次watch就可以忽略这些资源了
			m, err := meta.Accessor(event.Object)
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("%s: unable to understand watch event %#v", r.name, event))
				continue
			}
			newResourceVersion := m.GetResourceVersion()
			// 调用store(delta_fifo队列)根据event_type执行相应的操作
			switch event.Type {
			case watch.Added:
				err := r.store.Add(event.Object)
				if err != nil {
					utilruntime.HandleError(fmt.Errorf("%s: unable to add watch event object (%#v) to store: %v", r.name, event.Object, err))
				}
			case watch.Modified:
				err := r.store.Update(event.Object)
				if err != nil {
					utilruntime.HandleError(fmt.Errorf("%s: unable to update watch event object (%#v) to store: %v", r.name, event.Object, err))
				}
			case watch.Deleted:
				err := r.store.Delete(event.Object)
				if err != nil {
					utilruntime.HandleError(fmt.Errorf("%s: unable to delete watch event object (%#v) from store: %v", r.name, event.Object, err))
				}
			case watch.Bookmark:
			default:
				utilruntime.HandleError(fmt.Errorf("%s: unable to understand watch event %#v", r.name, event))
			}
			// 更新最新资源版本
			*resourceVersion = newResourceVersion
			r.setLastSyncResourceVersion(newResourceVersion)
			eventCount++
		}
	}
	watchDuration := r.clock.Since(start)
	// 若watch时间小于1秒或者监听的event为0，则异常退出
	if watchDuration < 1*time.Second && eventCount == 0 {
		return fmt.Errorf("very short watch: %s: Unexpected watch close - watch lasted less than a second and no items received", r.name)
	}
	return nil
}

// 设置最新的资源版本
func (r *Reflector) setLastSyncResourceVersion(v string) {
	r.lastSyncResourceVersionMutex.Lock()
	defer r.lastSyncResourceVersionMutex.Unlock()
	r.lastSyncResourceVersion = v
}

// relistResourceVersion determines the resource version the reflector should list or relist from.
// Returns either the lastSyncResourceVersion so that this reflector will relist with a resource
// versions no older than has already been observed in relist results or watch events, or, if the last relist resulted
// in an HTTP 410 (Gone) status code, returns "" so that the relist will use the latest resource version available in
// etcd via a quorum read.
func (r *Reflector) relistResourceVersion() string {
	r.lastSyncResourceVersionMutex.RLock()
	defer r.lastSyncResourceVersionMutex.RUnlock()

	if r.isLastSyncResourceVersionGone {
		// Since this reflector makes paginated list requests, and all paginated list requests skip the watch cache
		// if the lastSyncResourceVersion is expired, we set ResourceVersion="" and list again to re-establish reflector
		// to the latest available ResourceVersion, using a consistent read from etcd.
		return ""
	}
	if r.lastSyncResourceVersion == "" {
		// For performance reasons, initial list performed by reflector uses "0" as resource version to allow it to
		// be served from the watch cache if it is enabled.
		return "0"
	}
	return r.lastSyncResourceVersion
}

// setIsLastSyncResourceVersionExpired sets if the last list or watch request with lastSyncResourceVersion returned a
// expired error: HTTP 410 (Gone) Status Code.
func (r *Reflector) setIsLastSyncResourceVersionExpired(isExpired bool) {
	r.lastSyncResourceVersionMutex.Lock()
	defer r.lastSyncResourceVersionMutex.Unlock()
	r.isLastSyncResourceVersionGone = isExpired
}

// 设置想要监听的类型
func (r *Reflector) setExpectedType(expectedType interface{}) {
	r.expectedType = reflect.TypeOf(expectedType)
	if r.expectedType == nil {
		r.expectedTypeName = defaultExpectedTypeName
		return
	}
	r.expectedTypeName = r.expectedType.String()
	if obj, ok := expectedType.(*unstructured.Unstructured); ok {
		// Use gvk to check that watch event objects are of the desired type.
		gvk := obj.GroupVersionKind()
		if gvk.Empty() {
			klog.V(4).Infof("Reflector from %s configured with expectedType of *unstructured.Unstructured with empty GroupVersionKind.", r.name)
			return
		}
		r.expectedGVK = &gvk
		r.expectedTypeName = gvk.String()
	}
}

func isExpiredError(err error) bool {
	// In Kubernetes 1.17 and earlier, the api server returns both apierrors.StatusReasonExpired and
	// apierrors.StatusReasonGone for HTTP 410 (Gone) status code responses. In 1.18 the kube server is more consistent
	// and always returns apierrors.StatusReasonExpired. For backward compatibility we can only remove the apierrors.IsGone
	// check when we fully drop support for Kubernetes 1.17 servers from reflectors.
	return apierrors.IsResourceExpired(err) || apierrors.IsGone(err)
}
