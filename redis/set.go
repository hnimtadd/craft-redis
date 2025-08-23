package redis

import (
	"sync"
)

type (
	Set[T any] struct {
		data *sync.Map

		factoryFunc ElementFactory[T]
	}
	ElementFactory[T any] func() *T
)

func NewBLSet[T any](factory ElementFactory[T]) *Set[T] {
	return &Set[T]{
		data:        &sync.Map{},
		factoryFunc: factory,
	}
}

func (s *Set[T]) Set(key string, data *T) (*T, bool) {
	_, found := s.data.Load(key)
	if found {
		return nil, false
	}

	s.data.Store(key, data)
	return data, true
}

// Get return the value at the key key, if the key is not exists,
// then s will generate a new element at key and return
// the second return value indicate if new value is created or not.
func (s *Set[T]) Get(key string) (*T, bool) {
	data, found := s.data.Load(key)
	if !found {
		ele := s.factoryFunc()
		addedEle, _ := s.data.LoadOrStore(key, ele)
		return addedEle.(*T), true
	}
	return data.(*T), false
}

func (s *Set[T]) Has(key string) bool {
	_, found := s.data.Load(key)
	return found
}
