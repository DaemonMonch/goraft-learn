package goraft

import (
	"os"
	"regexp"
	"sort"
	"testing"
	"unsafe"
)

func TestNewLogFile(t *testing.T) {
	config := &Config{MaxLogFileSize: 1024, LogFileDirPath: "."}
	log := &Log{
		Data:  []byte("hello"),
		Epoch: 1,
		Index: 1,
	}
	log2 := &Log{
		Data:  []byte("lakjfioehfoewhfiouwehfuioh"),
		Epoch: 1,
		Index: 2,
	}
	t.Log(int(config.MaxLogFileSize))
	logFile, err := newLogFile(config, log)
	if err != nil {
		t.Fatal(err)
	}
	logFile.append([]*Log{log, log2})

	logFile.traverse(func(epoch uint32, index uint64, data []byte) error {
		t.Logf("%d %d %s", epoch, index, string(data))
		return nil
	})

	_, err = logFile.close()
	if err != nil {
		t.Fatal(err)
	}

	err = os.Remove("1-1_1-2.log")
	if err != nil {
		t.Log(err)
	}
}

func Test1(t *testing.T) {
	r, _ := regexp.Compile("([0-9]+)-([0-9]+)_([0-9]+)-([0-9]+)")
	m := r.FindSubmatch([]byte("1-20_2-100.log"))

	for _, mm := range m {
		t.Log(string(mm))
	}
}

func TestUptr(t *testing.T) {
	type S struct {
		s   uint8
		ptr uintptr
	}
	ss := S{}
	t.Log(unsafe.Sizeof(ss))
	t.Log(unsafe.Alignof(ss))
	t.Log(unsafe.Offsetof(ss.s))
	d := []byte{11, 88, 22, 44}

	s := (*S)(unsafe.Pointer(&d[0]))
	t.Log(*((*uint8)(unsafe.Pointer(&s.ptr))))
}

func TestSearch1(t *testing.T) {
	a := []int{1, 2, 3, 4}
	f := 0
	i := sort.Search(len(a), func(i int) bool { return a[i] >= f })
	t.Log(i)
}

func TestSort(t *testing.T) {
	ls := LogFiles([]*LogFile{
		&LogFile{startEpoch: 1, endEpoch: 1, startIndex: 21, endIndex: 40},
		&LogFile{startEpoch: 1, endEpoch: 1, startIndex: 1, endIndex: 20},
		&LogFile{startEpoch: 1, endEpoch: 2, startIndex: 41, endIndex: 60},
		&LogFile{startEpoch: 5, endEpoch: 6, startIndex: 81, endIndex: 100},
		&LogFile{startEpoch: 2, endEpoch: 5, startIndex: 61, endIndex: 80},
		&LogFile{startEpoch: 8, endEpoch: 12, startIndex: 131, endIndex: 150},
		&LogFile{startEpoch: 6, endEpoch: 6, startIndex: 101, endIndex: 130},
	})

	sort.Sort(ls)
	for _, l := range ls {
		t.Log(l.startIndex)
	}
	if ls[0].startIndex != 1 {
		t.Fail()
	}
}

func TestSearch(t *testing.T) {
	ls := LogFiles([]*LogFile{
		&LogFile{startEpoch: 1, endEpoch: 1, startIndex: 1, endIndex: 20},
		&LogFile{startEpoch: 1, endEpoch: 1, startIndex: 21, endIndex: 40},
		&LogFile{startEpoch: 1, endEpoch: 2, startIndex: 41, endIndex: 60},
		&LogFile{startEpoch: 2, endEpoch: 5, startIndex: 61, endIndex: 80},
		&LogFile{startEpoch: 5, endEpoch: 6, startIndex: 81, endIndex: 100},
		&LogFile{startEpoch: 6, endEpoch: 6, startIndex: 101, endIndex: 130},
		&LogFile{startEpoch: 8, endEpoch: 12, startIndex: 131, endIndex: 150},
	})

	epoch := 4
	index := 70

	f := func(i int) bool {
		n := ls[i]
		return (n.startEpoch > uint32(epoch) && n.startIndex > uint64(index)) || (n.endEpoch >= uint32(epoch) && n.endIndex >= uint64(index))
	}

	i := sort.Search(len(ls), f)

	if i != 3 {
		t.Fatal(i)
	}

	epoch = 6
	index = 101
	i = sort.Search(len(ls), f)

	if i != 5 {
		t.Fatal(i)
	}

	epoch = 12
	index = 150
	i = sort.Search(len(ls), f)

	if i != 6 {
		t.Fatal(i)
	}

	epoch = 16
	index = 240
	i = sort.Search(len(ls), f)

	if i != 7 {
		t.Fatal(i)
	}

	epoch = 4
	index = 240
	i = sort.Search(len(ls), f)

	if i != 7 {
		t.Fatal(i)
	}

	epoch = 1
	index = 0
	i = sort.Search(len(ls), f)

	if i != 0 {
		t.Fatal(i)
	}
}
