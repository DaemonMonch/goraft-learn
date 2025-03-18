package goraft

import (
	"io"
	"os"
	"path/filepath"
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
	m := r.FindStringSubmatch("1-20_2-100.log")

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

func BenchmarkLogStore1GFind(b *testing.B) {
	logger.SetOutput(io.Discard)
	store := NewFileLogStore(&Config{LogFileDirPath: ".", MaxLogFileSize: 1024 * 1024 * 1024})
	err := store.Init()
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < 1; i++ {
		store.Find(1, 10)
	}
	// b.ReportMetric()

	// b.ResetTimer()
	b.Run("find-1", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			store.Find(1, int64(1))
		}
	})
	// b.Log(b.Elapsed())

	// b.ResetTimer()
	b.Run("find-2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			store.Find(1, int64(512))
		}
	})

	b.Run("find-3", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			store.Find(1, int64(1023))
		}
	})
	// b.Log(b.Elapsed())
}

func TestLogStore1GOnlyFind(t *testing.T) {
	store := NewFileLogStore(&Config{LogFileDirPath: ".", MaxLogFileSize: 1024 * 1024 * 1024})
	err := store.Init()
	if err != nil {
		t.Fatal(err)
	}
	store.Find(1, 100)
}

func TestLogStore1G(t *testing.T) {
	// defer func() {
	// 	os.Remove("write")
	// 	os.Remove("write.offset")
	// 	fs, _ := filepath.Glob("*.log")
	// 	for _, v := range fs {
	// 		os.Remove(v)
	// 	}
	// }()

	logs := genLogs(1, 1024)
	var s int
	for _, v := range logs {
		s += len(v.Data) + 12
	}
	// t.Log(s)
	store := NewFileLogStore(&Config{LogFileDirPath: ".", MaxLogFileSize: 1024 * 1024 * 1024})
	err := store.Init()
	if err != nil {
		t.Fatal(err)
	}

	err = store.Append(logs)
	if err != nil {
		t.Fatal(err)
	}

	l, err := store.Find(1, 9)
	if err != nil {
		t.Fatal(err)
	}

	if l.Epoch != 1 || l.Index != 9 {
		t.Fail()
	}

}

func TestLogStore(t *testing.T) {
	defer func() {
		os.Remove("write")
		os.Remove("write.offset")
		fs, _ := filepath.Glob("*.log")
		for _, v := range fs {
			os.Remove(v)
		}
	}()
	logs := genLogs(1, 100)
	store := NewFileLogStore(&Config{LogFileDirPath: ".", MaxLogFileSize: 1024})
	err := store.Init()
	if err != nil {
		t.Fatal(err)
	}

	err = store.Append(logs)
	if err != nil {
		t.Fatal(err)
	}

	l, err := store.Find(1, 9)
	if err != nil {
		t.Fatal(err)
	}

	if l.Epoch != 1 || l.Index != 9 || string(l.Data) != "1-9" {
		t.Fail()
	}

	l, err = store.Find(1, 99)
	if err != nil {
		t.Fatal(err)
	}

	if l.Epoch != 1 || l.Index != 99 || string(l.Data) != "1-99" {
		t.Fail()
	}

	l, err = store.Find(1, 383)
	if err != nil {
		t.Fatal(err)
	}

	if l.Epoch != 1 || l.Index != 383 || string(l.Data) != "1-383" {
		t.Fail()
	}

}

func genLogs(epoch, count int) []*Log {
	var logs []*Log
	idx := 1

	data := [1024 * 1024]byte{}
	for i := 1; i <= epoch; i++ {
		for j := 0; j < count; j++ {
			logs = append(logs, &Log{Epoch: i, Index: int64(idx), Data: data[:]})
			idx++
		}
	}
	return logs
}
