package goraft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"syscall"
	"unsafe"
)

const (
	LOG_HEADER_MASK = 16
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

	offsetFile    *os.File
	mapOffsetFile []byte
}

func (l *LogFile) last() (*Log, error) {
	var log *Log
	l.traverse(func(ep uint32, idx uint64, d []byte) error {
		log = &Log{Epoch: int(ep), Index: int64(idx), Data: d}
		return errors.New("")
	})
	if log == nil {
		return nil, os.ErrNotExist
	}

	return log, nil
}

func (l *LogFile) validate() error {
	return nil
}

func (l *LogFile) close() (string, error) {
	l.endEpoch = binary.LittleEndian.Uint32(l.mapFile[l.mapFileOffset-4:])
	l.endIndex = binary.LittleEndian.Uint64(l.mapFile[l.mapFileOffset-12:])
	l.crc = crc32.ChecksumIEEE(l.mapFile)
	binary.LittleEndian.PutUint32(l.mapFile[l.mapFileOffset:], l.crc)
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
	l.f = nil
	l.mapFile = nil

	name := fmt.Sprintf("%d-%d_%d-%d.log", l.startEpoch, l.startIndex, l.endEpoch, l.endIndex)
	l.filePath = path.Join(l.config.LogFileDirPath, name)
	err = os.Rename(path.Join(l.config.LogFileDirPath, "write"), l.filePath)
	if err != nil {
		return "", err
	}

	binary.LittleEndian.PutUint64(l.mapOffsetFile, 0)

	return name, nil
}

func newLogFile(config *Config, firstLog *Log) (*LogFile, error) {
	f, err := os.OpenFile(path.Join(config.LogFileDirPath, "write"), os.O_CREATE|os.O_RDWR, os.ModePerm)
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
		binary.LittleEndian.PutUint32(l.mapFile[l.mapFileOffset:], uint32(len(log.Data)))
		l.mapFileOffset += 4
		binary.LittleEndian.PutUint64(l.mapFile[l.mapFileOffset:], uint64(log.Index))
		l.mapFileOffset += 8
		binary.LittleEndian.PutUint32(l.mapFile[l.mapFileOffset:], uint32(log.Epoch))
		l.mapFileOffset += 4

		l.endEpoch = uint32(log.Epoch)
		l.endIndex = uint64(log.Index)

		// logger.Printf("mapOffsetFile [%d]\n", len(l.mapOffsetFile))
		// binary.LittleEndian.PutUint64(l.mapOffsetFile, uint64(l.mapFileOffset))
		ptr := unsafe.Pointer(&l.mapOffsetFile[0])
		*((*uint64)(ptr)) = uint64(l.mapFileOffset)
	}

}

func initCurFile(config *Config) (*LogFile, error) {
	curFile, err := os.OpenFile(path.Join(config.LogFileDirPath, "write"), os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}

	offsetFile, err := os.OpenFile(path.Join(config.LogFileDirPath, "write.offset"), os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}

	err = offsetFile.Truncate(8)
	if err != nil {
		return nil, err
	}

	curFileOffset, err := syscall.Mmap(int(offsetFile.Fd()), 0, 8, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	err = curFile.Truncate(config.MaxLogFileSize + 4)
	if err != nil {
		return nil, err
	}

	mapFile, err := syscall.Mmap(int(curFile.Fd()), 0, int(config.MaxLogFileSize)+4, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)

	if err != nil {
		return nil, err
	}

	logFile := &LogFile{
		f:             curFile,
		mapFileOffset: int64(binary.LittleEndian.Uint64(curFileOffset)),
		filePath:      path.Join(config.LogFileDirPath, "write"),
		config:        config,
		mapFile:       mapFile,
		offsetFile:    offsetFile,
		mapOffsetFile: curFileOffset,
	}

	return logFile, nil
}

func (l *LogFile) flush() error {
	return nil
}

func (l *LogFile) find(epoch int, index int64) (*Log, error) {
	if l.f == nil {
		l.init()
	}
	var log *Log
	l.traverse(func(ep uint32, idx uint64, d []byte) error {
		if ep == uint32(epoch) && idx == uint64(index) {
			log = &Log{Epoch: epoch, Index: index, Data: d}
			return errors.New("")
		}
		return nil
	})
	if log == nil {
		return nil, os.ErrNotExist
	}

	return log, nil

}

func (l *LogFile) init() error {
	logger.Printf("init for file [%s]\n", l.filePath)
	f, err := os.OpenFile(l.filePath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return err
	}
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	logger.Printf("file size [%d]\n", stat.Size())
	mapFile, err := syscall.Mmap(int(f.Fd()), 0, int(l.config.MaxLogFileSize), syscall.PROT_READ, syscall.MAP_SHARED)

	if err != nil {
		return err
	}

	l.f = f
	l.mapFile = mapFile
	l.mapFileOffset = stat.Size() - 4
	logger.Printf("mapFileOffset [%d]\n", stat.Size()-4)
	return nil
}

func (l *LogFile) traverse(f func(epoch uint32, index uint64, data []byte) error) {

	for offset := l.mapFileOffset; offset > 0; offset -= (LOG_HEADER_MASK + int64(binary.LittleEndian.Uint32(l.mapFile[offset-LOG_HEADER_MASK:]))) {
		ep := binary.LittleEndian.Uint32(l.mapFile[offset-4:])
		idx := binary.LittleEndian.Uint64(l.mapFile[offset-12:])
		len := binary.LittleEndian.Uint32(l.mapFile[offset-LOG_HEADER_MASK:])
		d := l.mapFile[offset-int64(len)-LOG_HEADER_MASK : offset-LOG_HEADER_MASK]

		err := f(ep, idx, d)
		if err != nil {
			return
		}
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

	s.curFile, err = initCurFile(s.config)
	if err != nil {
		return err
	}

	return nil
}

func (s *FileLogStore) Append(logs []*Log) error {
	for _, log := range logs {
		if s.curFile.mapFileOffset+int64(len(log.Data)+LOG_HEADER_MASK) > s.config.MaxLogFileSize {
			_, err := s.curFile.close()
			if err != nil {
				return err
			}
			s.files = append(s.files, s.curFile)
			f, err := newLogFile(s.config, log)
			f.mapOffsetFile = s.curFile.mapOffsetFile
			f.offsetFile = s.curFile.offsetFile
			if err != nil {
				return err
			}
			s.curFile = f
		}
		s.curFile.append([]*Log{log})
	}

	return nil
}

func (s *FileLogStore) Last() (*Log, error) {
	return s.curFile.last()
}

func (s *FileLogStore) Find(epoch int, index int64) (*Log, error) {
	log, _ := s.curFile.find(epoch, index)
	if log != nil {
		return log, nil
	}

	fileIdex := sort.Search(len(s.files), func(i int) bool {
		n := s.files[i]
		return (n.startEpoch > uint32(epoch) && n.startIndex > uint64(index)) || (n.endEpoch >= uint32(epoch) && n.endIndex >= uint64(index))
	})

	logger.Printf("find in achieved file index [%d]\n", fileIdex)
	return s.files[fileIdex].find(epoch, index)
}

type LogFiles []*LogFile

func (l LogFiles) Len() int      { return len(l) }
func (l LogFiles) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l LogFiles) Less(i, j int) bool {
	return l[i].endEpoch < l[j].startEpoch || l[i].endIndex < l[j].startIndex
}

func leftMostBit(i int) int {
	return i
}
