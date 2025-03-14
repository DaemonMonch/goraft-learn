package goraft

import (
	"os"
	"regexp"
	"testing"
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
	_, err = logFile.close()
	if err != nil {
		t.Fatal(err)
	}

	err = os.Remove("1-1_1-2")
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
