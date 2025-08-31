package redis

import (
	"sync"
)

type (
	Set[T any] struct {
		data *sync.Map
	}
	ElementFactory[T any] func() *T
)

func NewBLSet[T any]() *Set[T] {
	return &Set[T]{
		data: &sync.Map{},
	}
}

// Set reuturn added data and boolean indicates if data is added successfuly.
func (s *Set[T]) Set(key string, data *T) (*T, bool) {
	_, found := s.data.Load(key)
	if found {
		return nil, false
	}

	s.data.Store(key, data)
	return data, true
}

func (s *Set[T]) Get(key string) (*T, bool) {
	data, found := s.data.Load(key)
	if !found {
		return nil, false
	}

	return data.(*T), true
}

func (s *Set[T]) Remove(key string) {
	s.data.Delete(key)
}

// Getsert return the value at the key key, if the key is not exists,
// then value will be used..
// The second value indicate if key is upserted or not.
func (s *Set[T]) Getsert(key string, value *T) (*T, bool) {
	data, found := s.data.Load(key)
	if !found {
		addedEle, _ := s.data.LoadOrStore(key, value)
		return addedEle.(*T), true
	}
	return data.(*T), false
}

func (s *Set[T]) Has(key string) bool {
	_, found := s.data.Load(key)
	return found
}

func (s *Set[T]) ForEach(handler func(string, *T) bool) {
	s.data.Range(func(key, value any) bool {
		keyStr, ok := key.(string)
		if !ok {
			return false
		}
		valT, ok := value.(*T)
		if !ok {
			return false
		}
		return handler(keyStr, valT)
	})
}
