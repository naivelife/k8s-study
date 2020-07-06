package cache

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/util/sets"
)

const (
	NamespaceIndex string = "namespace"
)

// Example:
// pod1的namespace为test_namespace
// pod2的namespace为test_namespace
// pod3的namespace为test_namespace_v2
// IndexFunc为MetaNamespaceIndexFunc
// Indexers的元素如下：
//     namespace -> MetaNamespaceIndexFunc
// Index(real_index)的元素如下
//     test_namespace -> {pod1, pod2}
//     test_namespace_v2 -> {pod3}
// Indices的元素如下
//     namespace -> real_index

// 计算索引的函数
type IndexFunc func(obj interface{}) ([]string, error)

// 所有的索引函数
// key为索引函数名称，value为索引函数
// keyFunc1Name -> keyFunc1
// keyFunc2Name -> keyFunc2
type Indexers map[string]IndexFunc

// 多个obj可能索引一样，索引需要一个map组织
// key为索引值，value为obj的key的集合
// index1 -> {obj1.key, obj2.key}
// index2 -> {obj3.key, obj4.key}
type Index map[string]sets.String

// 每一种计算索引的方式都需要一个map来进行数据存储
// 因此每一种索引的Index需要一个map来维护
// key为索引名称，value为Index(每种索引对应的存储)
// keyFunc1Name -> Index1
// keyFunc2Name -> Index2
type Indices map[string]Index

type Indexer interface {
	Store
	// indexName索引类，obj是对象，计算obj在indexName索引类中的索引键，通过索引键把所有的对象取出来
	// 基本就是获取符合obj特征的所有对象，所谓的特征就是对象在索引类中的索引键
	// 根据indexName去Indexers找到对应的IndexFunc，并通过给定obj计算indexValue
	// 根据indexName去Indices找到对应的Index，通过indexValue找到对应的set
	Index(indexName string, obj interface{}) ([]interface{}, error)
	// indexKey是indexName索引类中一个索引键，函数返回indexKey指定的所有对象键
	// 这个对象键是Indexer内唯一的，在添加的时候会计算，后面讲具体Indexer实例的会讲解
	// 根据indexName去Indices找到对应的Index
	// 根据indexedValue去Index找到对应的set
	IndexKeys(indexName, indexedValue string) ([]string, error)
	// 根据indexName去Indices找到对应的Index
	// 遍历返回所有的Index key
	ListIndexFuncValues(indexName string) []string
	// 这个函数和Index类似
	ByIndex(indexName, indexedValue string) ([]interface{}, error)
	// 返回Indexers
	GetIndexers() Indexers
	// 增加Indexers
	AddIndexers(newIndexers Indexers) error
}

// 默认的索引函数，基于namespace构建索引
func MetaNamespaceIndexFunc(obj interface{}) ([]string, error) {
	m, err := meta.Accessor(obj)
	if err != nil {
		return []string{""}, fmt.Errorf("object has no meta: %v", err)
	}
	return []string{m.GetNamespace()}, nil
}

// IndexFunc -> KeyFunc
// 当且仅当IndexFunc生成的index只有一个
func IndexFuncToKeyFuncAdapter(indexFunc IndexFunc) KeyFunc {
	return func(obj interface{}) (string, error) {
		indexKeys, err := indexFunc(obj)
		if err != nil {
			return "", err
		}
		if len(indexKeys) > 1 {
			return "", fmt.Errorf("too many keys: %v", indexKeys)
		}
		if len(indexKeys) == 0 {
			return "", fmt.Errorf("unexpected empty indexKeys")
		}
		return indexKeys[0], nil
	}
}