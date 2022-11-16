package utils

import (
	"bytes"
	"errors"
	"github.com/hardcore-os/corekv/utils/codec"
	"math/rand"
	"sync"
	"time"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	header *Element

	rand *rand.Rand

	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList() *SkipList {
	return &SkipList{
		header:   newElement(0, nil, defaultMaxLevel),
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		maxLevel: defaultMaxLevel,
		length:   0,
		lock:     sync.RWMutex{},
		size:     0,
	}
}

type Element struct {
	levels []*Element
	entry  *codec.Entry
	score  float64
}

func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(data *codec.Entry) error {
	//implement me here!!!
	if data == nil {
		return errors.New("dont add nil")
	}
	list.lock.Lock()
	defer list.lock.Unlock()
	score := list.calcScore(data.Key)
	preElemList := make([]*Element, defaultMaxLevel)
	preElem := list.header
	i := defaultMaxLevel - 1

	for i >= 0 {
		for next := preElem.levels[i]; next != nil; next = preElem.levels[i] {
			com := list.compare(score, data.Key, next)
			if com == 0 {
				next.entry = data
				return nil
			}
			if com > 0 {
				// find the insert position in this level
				break
			}
			if com < 0 {
				preElem = next
				preElemList[i] = preElem
			}
		}
		preElemList[i] = preElem
		i--
	}
	level := list.randLevel()
	insertElem := newElement(list.calcScore(data.Key), data, level)
	for i := 0; i < level; i++ {
		insertElem.levels[i] = preElemList[i].levels[i]
		preElemList[i].levels[i] = insertElem
	}
	list.length++
	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	//implement me here!!!
	if len(key) == 0 || list.length == 0 {
		return nil
	}
	list.lock.RLock()
	defer list.lock.RUnlock()
	score := list.calcScore(key)
	preElem := list.header
	i := defaultMaxLevel - 1

	// find from max level
	for i >= 0 {
		for next := preElem.levels[i]; next != nil; next = preElem.levels[i] {
			comp := list.compare(score, key, next)
			if comp == 0 {
				return next.entry
			} else if comp > 0 {
				// next key > needed key
				// decr level
				break
			} else {
				// next key < needed key
				// continue to find in this level
				preElem = next
			}
		}
		i -= 1
	}
	return nil
}

func (list *SkipList) Close() error {
	return nil
}

func (list *SkipList) calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

// compare curPtr key and finding key
// return 0 if next.key == key, 1 if next.key > key, -1 if next.key < key
func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)
	}
	if score < next.score {
		return -1
	}
	return 1
}

func (list *SkipList) randLevel() int {
	level := 1
	for {
		if list.rand.Int()%2 == 0 {
			level += 1
			continue
		}
		break
	}
	if level >= defaultMaxLevel {
		level = defaultMaxLevel - 1
	}
	return level
}

func (list *SkipList) Size() int64 {
	list.lock.RLock()
	s := list.size
	list.lock.RUnlock()
	return s
}
