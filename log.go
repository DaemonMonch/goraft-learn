package goraft

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"syscall"
)

type Log struct {
	Index int64
	Epoch int
	Data  []byte
}

type LogStore interface {
	Append(logs []*Log) error
	Last() (*Log, error)
	Find(epoch int, index int64) (*Log, error)
}

// data len index epoch
type LogFile struct {
	startEpoch    uint32
	endEpoch      uint32
	startIndex    uint64
	endIndex      uint64
	filePath      string
	f             *os.File
	config        *Config
	mapFile       []byte
	mapFileOffset int64
	crc           uint32
}

func (l *LogFile) validate() error {
	panic("unimplemented")
}

func (l *LogFile) close() (string, error) {
	l.endEpoch = binary.BigEndian.Uint32(l.mapFile[l.mapFileOffset-4:])
	l.endIndex = binary.BigEndian.Uint64(l.mapFile[l.mapFileOffset-12:])
	l.crc = crc32.ChecksumIEEE(l.mapFile)
	binary.BigEndian.PutUint32(l.mapFile[l.mapFileOffset:], l.crc)
	l.mapFileOffset += 4
	err := syscall.Munmap(l.mapFile)
	if err != nil {
		return "", err
	}

	l.f.Truncate(l.mapFileOffset)
	err = l.f.Close()
	if err != nil {
		return "", err
	}

	name := fmt.Sprintf("%d-%d_%d-%d.log", l.startEpoch, l.startIndex, l.endEpoch, l.endIndex)
	err = os.Rename(path.Join(l.config.LogFileDirPath, fmt.Sprintf("%d-%d_", l.startEpoch, l.startIndex)), path.Join(l.config.LogFileDirPath, name))
	if err != nil {
		return "", err
	}

	return name, nil
}

func newLogFile(config *Config, firstLog *Log) (*LogFile, error) {
	f, err := os.OpenFile(path.Join(config.LogFileDirPath, fmt.Sprintf("%d-%d_", firstLog.Epoch, firstLog.Index)), os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}

	logFile := &LogFile{
		startEpoch: uint32(firstLog.Epoch),
		startIndex: uint64(firstLog.Index),
		f:          f,
		config:     config,
	}

	err = f.Truncate(config.MaxLogFileSize + 4)
	if err != nil {
		return nil, err
	}
	logFile.mapFile, err = syscall.Mmap(int(f.Fd()), 0, int(config.MaxLogFileSize)+4, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return logFile, nil
}

func (l *LogFile) append(logs []*Log) {
	for _, log := range logs {

		l.mapFileOffset += int64(copy(l.mapFile[l.mapFileOffset:], log.Data))
		binary.BigEndian.PutUint32(l.mapFile[l.mapFileOffset:], uint32(len(log.Data)))
		l.mapFileOffset += 4
		binary.BigEndian.PutUint64(l.mapFile[l.mapFileOffset:], uint64(log.Index))
		l.mapFileOffset += 8
		binary.BigEndian.PutUint32(l.mapFile[l.mapFileOffset:], uint32(log.Epoch))
		l.mapFileOffset += 4
	}
}

type FileLogStore struct {
	config  *Config
	files   []*LogFile
	curFile *LogFile
}

func NewFileLogStore(config *Config) *FileLogStore {
	return &FileLogStore{
		config: config,
	}
}

func (s *FileLogStore) Init() error {
	logs, err := filepath.Glob("*.log")
	if err != nil {
		return err
	}

	r, _ := regexp.Compile("([0-9]+)-([0-9]+)_([0-9]+)-([0-9]+)")
	for _, log := range logs {
		name := filepath.Base(log)
		m := r.FindStringSubmatch(name)
		se, _ := strconv.Atoi(m[0])
		si, _ := strconv.ParseInt(m[1], 10, 0)
		ee, _ := strconv.Atoi(m[2])
		ei, _ := strconv.ParseInt(m[3], 10, 0)
		logFile := &LogFile{
			startEpoch: uint32(se),
			startIndex: uint64(si),
			endEpoch:   uint32(ee),
			endIndex:   uint64(ei),
			filePath:   path.Join(s.config.LogFileDirPath, log),
		}
		err := logFile.validate()
		if err != nil {
			return err
		}

		s.files = append(s.files, logFile)
	}
	sort.Sort(LogFiles(s.files))
	return nil
}

func (s *FileLogStore) Append(logs []*Log) error {
	var size int
	for _, log := range logs {
		size += len(log.Data) + 16
	}
	if s.curFile.mapFileOffset+int64(size) > s.config.MaxLogFileSize {
		_, err := s.curFile.close()
		if err != nil {
			return err
		}
		s.files = append(s.files, s.curFile)
		f, err := s.newLogFile(logs[0])
		if err != nil {
			return err
		}
		s.curFile = f
	}
	s.curFile.append(logs)
	return nil
}

func (s *FileLogStore) newLogFile(firstLog *Log) (*LogFile, error) {
	return newLogFile(s.config, firstLog)
}

type LogFiles []*LogFile

func (l LogFiles) Len() int      { return len(l) }
func (l LogFiles) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l LogFiles) Less(i, j int) bool {
	return l[i].startEpoch < l[j].startEpoch && l[i].startIndex < l[j].startIndex
}
