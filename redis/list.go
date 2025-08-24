package redis

import (
	"container/list"
	"errors"
	"fmt"
	"slices"
	"sync"
)

type BLList[T any] struct {
	// []resp.data
	data *list.List

	subscribers []*Subscription
	mu          sync.Mutex
}
type Subscription struct {
	cond        *sync.Cond
	deactivated bool
}

func NewBLList[T any]() *BLList[T] {
	return &BLList[T]{
		data:        list.New(),
		subscribers: []*Subscription{},
	}
}

func (l *BLList[T]) get(idx uint) *list.Element {
	var (
		lOffset = int(idx)
		rOffset = l.data.Len() - 1 - int(idx)
		node    *list.Element
	)
	fmt.Println(lOffset, rOffset)

	switch {
	case rOffset < lOffset:
		// means if we traverse from last element, it's faster
		node = l.data.Back()
		for range rOffset {
			node = node.Prev()
		}

	default:
		node = l.data.Front()
		for range lOffset {
			node = node.Next()
		}
	}

	return node
}

func (l *BLList[T]) Append(eles ...T) int {
	for value := range slices.Values(eles) {
		l.data.PushBack(&value)
	}
	return l.data.Len()
}

func (l *BLList[T]) Prepend(value T) int {
	l.data.PushFront(&value)
	return l.data.Len()
}

func (l *BLList[T]) At(idx int) *T {
	if idx < 0 {
		return nil
	}
	if idx >= l.data.Len() {
		panic("out of length")
	}

	node := l.get(uint(idx))
	return node.Value.(*T)
}

func (l *BLList[T]) Remove(idx int) (*T, error) {
	if idx >= l.data.Len() {
		return nil, errors.New("out of length")
	}

	node := l.get(uint(idx))
	if node == nil {
		return nil, errors.New("something wrong")
	}

	value := l.data.Remove(node)
	return value.(*T), nil
}

// Slice returns list sub array [start, end)
func (l *BLList[T]) Slice(start, end uint) ([]*T, error) {
	if start > end {
		return nil, errors.New("invalid args, start must smaller than end")
	}
	if start >= uint(l.data.Len()) || end > uint(l.data.Len()) {
		return nil, errors.New("out of length")
	}
	left := l.get(start)
	if left == nil {
		return nil, errors.New("something wrong")
	}
	eles := make([]*T, end-start)
	node := left
	for i := range end - start {
		eles[i] = node.Value.(*T)
		node = node.Next()
	}
	return eles, nil
}

func (l *BLList[T]) Len() int {
	return l.data.Len()
}

func (l *BLList[T]) Signal() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for len(l.subscribers) > 0 {
		sub := l.subscribers[0]
		if sub.deactivated {
			l.subscribers = l.subscribers[1:]
			continue
		}
		fmt.Println("signalling", l.subscribers)
		sub.cond.Signal()
		l.subscribers = l.subscribers[1:]
		return
	}
}

func (l *BLList[T]) NewSubscription() *Subscription {
	sub := &Subscription{
		cond: sync.NewCond(&sync.Mutex{}),
	}
	return sub
}

func (l *BLList[T]) Subscribe(sub *Subscription) *Subscription {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Println("handling subscription")
	l.subscribers = append(l.subscribers, sub)
	return sub
}

func (l *BLList[T]) ForEach(fn func(*T) bool) {
	for curr := l.data.Front(); curr != nil; curr = curr.Next() {
		if shouldStop := fn(curr.Value.(*T)); shouldStop {
			break
		}
	}
}

func (s *Subscription) Deactivate() {
	s.deactivated = true
}

func (s *Subscription) Wait() {
	s.cond.L.Lock()
	s.cond.Wait()
	s.cond.L.Unlock()
}
