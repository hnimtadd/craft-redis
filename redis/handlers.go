package redis

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/redis/resp"
	"github.com/codecrafters-io/redis-starter-go/utils"
)

func (c *Controller) handleSet(key resp.BulkStringData, value resp.BulkStringData, opts ...resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	if len(opts)%2 != 0 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "syntax error",
		}
	}
	record := SetValueString{
		Data: value,
	}
	if len(opts) > 0 {
		for keyIdx, valIdx := 0, 1; valIdx < len(opts); keyIdx, valIdx = keyIdx+1, valIdx+1 {
			optKey := opts[keyIdx]
			optVal := opts[valIdx]
			switch strings.ToLower(optKey.Data) {
			case "px":
				// valIdx is 0-based
				ttlInMs, err := strconv.ParseInt(optVal.Data, 10, 64)
				if err != nil {
					return nil, &resp.SimpleErrorData{
						Type: resp.SimpleErrorTypeGeneric,
						Msg:  "invalid expire time in 'set' command",
					}
				}
				record.Timeout = time.Now().Add(time.Millisecond * time.Duration(ttlInMs))
			}
		}
	}
	c.data.Set(resp.Raw(key), &Value{
		Type: SetValueTypeString,
		Data: &record,
	})
	return resp.SimpleStringData{Data: "OK"}, nil
}

func (c *Controller) handleGet(key resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	valueData, found := c.data.Get(resp.Raw(key))
	if !found {
		return resp.NullBulkStringData{}, nil
	}
	if valueData.Type != SetValueTypeString {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeWrongType,
			Msg:  fmt.Sprintf("Operation against %s key holding the wrong kind of value", key.Data),
		}
	}
	record := valueData.Data.(*SetValueString)
	if record.isExpired {
		return resp.NullBulkStringData{}, nil
	}
	if !record.Timeout.IsZero() &&
		record.Timeout.Before(time.Now()) {
		record.isExpired = true
		return resp.NullBulkStringData{}, nil
	}
	return record.Data, nil
}

func (c *Controller) handleRPUSH(key resp.BulkStringData, values ...resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	valueData, _ := c.data.Getsert(resp.Raw(key), &Value{
		Type: SetValueTypeList,
		Data: NewListValue(),
	})
	if valueData.Type != SetValueTypeList {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeWrongType,
			Msg:  fmt.Sprintf("Operation against %s key holding the wrong kind of value", key.Data),
		}
	}
	lst := valueData.Data.(*SetValueList)
	defer func() {
		sub := lst.FirstAvailableSubscriber()
		if sub != nil {
			sub.Signal()
		}
	}()
	len := lst.Append(values...)

	return resp.Integer{Data: len}, nil
}

func (c *Controller) handleLPUSH(key resp.BulkStringData, values ...resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	value, _ := c.data.Getsert(resp.Raw(key), &Value{
		Type: SetValueTypeList,
		Data: NewListValue(),
	})
	if value.Type != SetValueTypeList {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeWrongType,
			Msg:  fmt.Sprintf("Operation against %s key holding the wrong kind of value", key.Data),
		}
	}
	lst := value.Data.(*SetValueList)
	defer func() {
		sub := lst.FirstAvailableSubscriber()
		if sub != nil {
			sub.Signal()
		}
	}()

	// reverse so we have a list that should be exists after we add to the list
	// then we just simply append the original list.
	for _, data := range values {
		lst.Prepend(data)
	}

	return resp.Integer{Data: lst.Len()}, nil
}

func (c *Controller) handleLRANGE(key resp.BulkStringData, from, to int) (resp.Data, *resp.SimpleErrorData) {
	valueData, found := c.data.Get(resp.Raw(key))
	if !found {
		return resp.ArraysData{}, nil
	}
	if valueData.Type != SetValueTypeList {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeWrongType,
			Msg:  fmt.Sprintf("Operation against %s key holding the wrong kind of value", key.Data),
		}
	}
	lst := valueData.Data.(*SetValueList)
	if from < 0 {
		from = lst.Len() + from
		from = max(from, 0)
	}
	if to < 0 {
		to = lst.Len() + to
		to = max(to, 0)
	}

	if from > to {
		return resp.ArraysData{}, nil
	}

	if from > lst.Len() {
		return resp.ArraysData{}, nil
	}

	to = min(to, lst.Len()-1)
	// from, to in redis is inclusive, but golang list slice is exclusive
	// so we get slice with [from, to+1)
	eles, err := lst.Slice(uint(from), uint(to+1))
	if err != nil {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeWrongType,
			Msg:  fmt.Sprintf("cannot get element: %v", err),
		}
	}
	results := make([]resp.Data, len(eles))
	for idx, ele := range eles {
		results[idx] = *ele
	}
	return resp.ArraysData{
		Datas: results,
	}, nil
}

func (c *Controller) handleLLEN(key resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	value, found := c.data.Get(resp.Raw(key))
	if !found {
		return resp.Integer{Data: 0}, nil
	}
	if value.Type != SetValueTypeList {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeWrongType,
			Msg:  fmt.Sprintf("Operation against %s key holding the wrong kind of value", key.Data),
		}
	}
	lst := value.Data.(*SetValueList)
	return resp.Integer{Data: lst.Len()}, nil
}

func (c *Controller) handleLPOP(key resp.BulkStringData, numItem uint64) (resp.Data, *resp.SimpleErrorData) {
	value, found := c.data.Get(resp.Raw(key))
	if !found {
		return resp.ArraysData{}, nil
	}
	if value.Type != SetValueTypeList {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeWrongType,
			Msg:  fmt.Sprintf("Operation against %s key holding the wrong kind of value", key.Data),
		}
	}
	lst := value.Data.(*SetValueList)
	if lst.Len() == 0 {
		return resp.BulkStringData{}, nil
	}

	switch numItem {
	case 1:
		popItem, err := lst.Remove(0)
		if err != nil {
			return nil, &resp.SimpleErrorData{
				Type: resp.SimpleErrorTypeGeneric,
				Msg:  fmt.Sprintf("Failed to remove element %v", err),
			}
		}
		return *popItem, nil
	default:
		numItem = min(numItem, uint64(lst.Len()))
		popItems := make([]resp.Data, numItem)

		for idx := range numItem {
			popItem, err := lst.Remove(0)
			if err != nil {
				return nil, &resp.SimpleErrorData{
					Type: resp.SimpleErrorTypeGeneric,
					Msg:  fmt.Sprintf("Failed to remove element %v", err),
				}
			}
			popItems[idx] = *popItem
		}

		return resp.ArraysData{
			Datas: popItems,
		}, nil
	}
}

func (c *Controller) handleBLPOP(keys []resp.BulkStringData, timeoutInMs int64) (resp.Data, *resp.SimpleErrorData) {
	cancelCh := make(chan any)
	doneCh := make(chan resp.BulkStringData, 1)
	for _, keyData := range keys {
		go func(key resp.BulkStringData) {
			value, _ := c.data.Getsert(resp.Raw(key), &Value{
				Type: SetValueTypeList,
				Data: NewListValue(),
			})
			if value.Type != SetValueTypeList {
				return
			}
			lst := value.Data.(*SetValueList)
			if lst.Len() > 0 {
				select {
				case doneCh <- key:
					fmt.Printf("Routine for %s sent signal.\n", resp.Raw(key))
				case <-cancelCh:
					fmt.Printf("Routine for %s found another signal was already sent and was canceled.\n", resp.Raw(key))
				}
			}
			sub := lst.NewSubscription()
			select {
			case <-cancelCh:
				fmt.Printf("Routine for %s was canceled before it was signaled.\n", resp.Raw(key))
				return // Exit gracefully.
			default:
				// Not canceled yet, proceed to wait.
			}

			sub = lst.Subscribe(sub)
			waitCh := make(chan struct{})
			go func() {
				sub.Wait()
				close(waitCh)
			}()

			select {
			case <-waitCh:
				fmt.Printf("Routine for %s has been woken up.\n", resp.Raw(key))
				// Try to send on the done channel.
				select {
				case doneCh <- key:
					fmt.Printf("Routine for %s sent signal.\n", resp.Raw(key))
				case <-cancelCh:
					sub.Signal()
					// If a cancel signal arrived while we were trying to send,
					// it means another routine won the race.
					fmt.Printf("Routine for %s found another signal was already sent and was canceled.\n", resp.Raw(key))
				}
			case <-cancelCh:
				// At this point, the goroutine from `cond.Wait()` is still blocked.
				// We need to to signal, in order to waky it up
				sub.Signal()
				fmt.Printf("Routine for %s was canceled while waiting.\n", resp.Raw(key))
				// At this point, the goroutine from `cond.Wait()` is still blocked.
				// There is no clean way to unblock a `cond.Wait()` from outside.
				// This is why the cleanup method is critical for the `A` struct to work.
			}
			sub.Deactivate()
		}(keyData)
	}
	defer close(cancelCh)

	if timeoutInMs > 0 {
		select {
		case key := <-doneCh:
			value, found := c.data.Get(resp.Raw(key))
			if !found {
				return resp.ArraysData{}, nil
			}
			if value.Type != SetValueTypeList {
				return nil, &resp.SimpleErrorData{
					Type: resp.SimpleErrorTypeWrongType,
					Msg:  fmt.Sprintf("Operation against %s key holding the wrong kind of value", key.Data),
				}
			}
			lst := value.Data.(*SetValueList)
			ele, err := lst.Remove(0)
			if err != nil {
				return nil, &resp.SimpleErrorData{
					Type: resp.SimpleErrorTypeGeneric,
					Msg:  fmt.Sprintf("Failed to remove element %v", err),
				}
			}
			return resp.ArraysData{
				Datas: []resp.Data{key, *ele},
			}, nil
		case <-time.After(time.Duration(timeoutInMs) * time.Millisecond):
			return resp.NullBulkStringData{}, nil
		}
	} else {
		key := <-doneCh
		value, found := c.data.Get(resp.Raw(key))
		if !found {
			return resp.ArraysData{}, nil
		}
		if value.Type != SetValueTypeList {
			return nil, &resp.SimpleErrorData{
				Type: resp.SimpleErrorTypeWrongType,
				Msg:  fmt.Sprintf("Operation against %s key holding the wrong kind of value", key.Data),
			}
		}
		lst := value.Data.(*SetValueList)
		ele, err := lst.Remove(0)
		if err != nil {
			return nil, &resp.SimpleErrorData{
				Type: resp.SimpleErrorTypeGeneric,
				Msg:  fmt.Sprintf("Failed to remove element %v", err),
			}
		}
		return resp.ArraysData{
			Datas: []resp.Data{key, *ele},
		}, nil
	}
}

func (c *Controller) handleTYPE(key resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	value, found := c.data.Get(resp.Raw(key))
	if !found {
		return resp.SimpleStringData{Data: "none"}, nil
	}
	return resp.SimpleStringData{Data: string(value.Type)}, nil
}

func (c *Controller) handleXADD(key resp.BulkStringData, entryID InputEntryID, kvs []resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	c.logger.Debug(resp.Raw(key))
	value, _ := c.data.Getsert(resp.Raw(key), &Value{
		Type: SetValueTypeStream,
		Data: NewStreamValue(),
	})
	if value.Type != SetValueTypeStream {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeWrongType,
			Msg:  "Operation against a key holding the wrong kind of value",
		}
	}
	stream := value.Data.(*SetValueStream)
	utils.Assert(stream != nil, "stream must not be nil here")
	defer func() {
		sub := stream.SharedSubscription()
		if sub != nil {
			sub.Broadcast()
		}
	}()
	validEntryID, err := fullfillStreamEntryID(stream, entryID)
	if err != nil {
		return nil, err
	}

	entry := StreamEntry{
		ID:  validEntryID,
		KVs: kvs,
	}
	lastElement := stream.Last()
	if lastElement != nil {
		if validEntryID.Cmp(lastElement.ID) <= 0 {
			return nil, &resp.SimpleErrorData{
				Type: resp.SimpleErrorTypeGeneric,
				Msg:  "The ID specified in XADD is equal or smaller than the target stream top item",
			}
		}
	}
	stream.Append(entry)
	return validEntryID.Data(), nil
}

func (c *Controller) handleXRANGE(key resp.BulkStringData, start, end EntryID) (resp.Data, *resp.SimpleErrorData) {
	c.logger.Debug(resp.Raw(key), start, end)
	value, found := c.data.Get(resp.Raw(key))
	if !found {
		return resp.ArraysData{}, nil
	}
	if value.Type != SetValueTypeStream {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeWrongType,
			Msg:  "Operation against a key holding the wrong kind of value",
		}
	}
	stream := value.Data.(*SetValueStream)
	if stream.Len() == 0 {
		return resp.ArraysData{}, nil
	}
	entries := []StreamEntry{}
	stream.ForEach(func(se *StreamEntry) bool {
		if start.Cmp(se.ID) <= 0 && end.Cmp(se.ID) >= 0 {
			entries = append(entries, *se)
			return false
		}
		if end.Cmp(se.ID) < 0 {
			return true
		}
		return false
	})
	datas := make([]resp.Data, len(entries))
	for idx, entry := range entries {
		kvsData := make([]resp.Data, len(entry.KVs))
		for idx, ele := range entry.KVs {
			kvsData[idx] = ele
		}
		datas[idx] = resp.ArraysData{
			Datas: []resp.Data{
				entry.ID.Data(),
				resp.ArraysData{
					Datas: kvsData,
				},
			},
		}
	}
	return resp.ArraysData{
		Datas: datas,
	}, nil
}

func (c *Controller) handleXREAD(keys []resp.BulkStringData, entriesID []EntryID, timeoutInMs *int64) (resp.Data, *resp.SimpleErrorData) {
	c.logger.Debug(len(keys), len(entriesID))
	utils.Assert(len(keys) == len(entriesID))
	cancelCh := make(chan any)
	doneCh := make(chan struct {
		key     resp.BulkStringData
		entryID EntryID
	}, len(keys))
	for idx, keyData := range keys {
		go func(key resp.BulkStringData, entryID EntryID) {
			value, _ := c.data.Getsert(resp.Raw(key), &Value{
				Type: SetValueTypeList,
				Data: NewStreamValue(),
			})
			if value.Type != SetValueTypeStream {
				return
			}
			stream := value.Data.(*SetValueStream)
			select {
			case <-cancelCh:
				fmt.Printf("Routine for %s was canceled before it was signaled.\n", resp.Raw(key))
				return // Exit gracefully.
			default:
				// Not canceled yet, proceed to wait.
			}
			waitCh := make(chan struct{})
			sub := stream.SharedSubscription()
			go func() {
				for {
					if stream.Len() > 0 {
						last := stream.Last()
						// if a stream has last record with greater entryID, then
						// at least we have 1 valid entry.
						if last.ID.Cmp(entryID) > 0 {
							close(waitCh)
							return
						}
					}
					sub.Wait()
				}
			}()
			select {
			case <-waitCh:
				fmt.Printf("Routine for %s has been woken up.\n", resp.Raw(key))
				// Try to send on the done channel.
				select {
				case doneCh <- struct {
					key     resp.BulkStringData
					entryID EntryID
				}{
					key:     key,
					entryID: entryID,
				}:
					fmt.Printf("Routine for %s sent signal.\n", resp.Raw(key))
				case <-cancelCh:
					fmt.Printf("Routine for %s found another signal was already sent and was canceled.\n", resp.Raw(key))
				}
			case <-cancelCh:
				fmt.Printf("Routine for %s was canceled while waiting.\n", resp.Raw(key))
			}
		}(keyData, entriesID[idx])
	}

	if timeoutInMs != nil {
		go func() {
			<-time.After(time.Duration(*timeoutInMs) * time.Millisecond)
			close(cancelCh)
		}()
	}

	var results resp.ArraysData
	for range len(keys) {
		select {
		case <-cancelCh:
			return resp.ArraysData{}, nil
		case record := <-doneCh:
			key := record.key
			entryID := record.entryID
			value, found := c.data.Get(resp.Raw(key))
			if !found {
				continue
			}
			if value.Type != SetValueTypeStream {
				continue
			}
			stream := value.Data.(*SetValueStream)
			if stream.Len() == 0 {
				continue
			}
			entries := []StreamEntry{}
			stream.ForEach(func(se *StreamEntry) bool {
				if entryID.Cmp(se.ID) < 0 {
					entries = append(entries, *se)
					return false
				}
				return false
			})
			c.logger.Debug("got", len(entries))
			datas := make([]resp.Data, len(entries))
			for idx, entry := range entries {
				kvsData := make([]resp.Data, len(entry.KVs))
				for idx, ele := range entry.KVs {
					kvsData[idx] = ele
				}
				datas[idx] = resp.ArraysData{
					Datas: []resp.Data{
						entry.ID.Data(),
						resp.ArraysData{
							Datas: kvsData,
						},
					},
				}
			}
			results.Datas = append(
				results.Datas,
				resp.ArraysData{
					Datas: []resp.Data{key, resp.ArraysData{Datas: datas}},
				})
		}
	}
	return results, nil
}
