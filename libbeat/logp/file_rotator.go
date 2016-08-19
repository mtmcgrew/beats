package logp

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"archive/zip"
	"bufio"
	"time"
	"sort"
)

const RotatorMaxFiles = 1024
const DefaultKeepFiles = 7
const DefaultRotateEveryBytes = 10 * 1024 * 1024

type FileRotator struct {
	Path             string
	Name             string
	RotateEveryBytes *uint64
	KeepFiles        *int

	current      *os.File
	current_size uint64
}

func (rotator *FileRotator) CreateDirectory() error {
	fileinfo, err := os.Stat(rotator.Path)
	if err == nil {
		if !fileinfo.IsDir() {
			return fmt.Errorf("%s exists but it's not a directory", rotator.Path)
		}
	}

	if os.IsNotExist(err) {
		err = os.MkdirAll(rotator.Path, 0755)
		if err != nil {
			return err
		}
	}

	return nil
}

func (rotator *FileRotator) CheckIfConfigSane() error {
	if len(rotator.Name) == 0 {
		return fmt.Errorf("File logging requires a name for the file names")
	}
	if rotator.KeepFiles == nil {
		rotator.KeepFiles = new(int)
		*rotator.KeepFiles = DefaultKeepFiles
	}
	if rotator.RotateEveryBytes == nil {
		rotator.RotateEveryBytes = new(uint64)
		*rotator.RotateEveryBytes = DefaultRotateEveryBytes
	}

	if *rotator.KeepFiles < 2 || *rotator.KeepFiles >= RotatorMaxFiles {
		return fmt.Errorf("The number of files to keep should be between 2 and %d", RotatorMaxFiles-1)
	}
	return nil
}

func (rotator *FileRotator) WriteLine(line []byte) error {
	if rotator.shouldRotate() {
		err := rotator.Rotate()
		if err != nil {
			return err
		}
	}

	line = append(line, '\n')
	_, err := rotator.current.Write(line)
	if err != nil {
		return err
	}
	rotator.current_size += uint64(len(line))

	return nil
}

func (rotator *FileRotator) shouldRotate() bool {
	if rotator.current == nil {
		return true
	}

	if rotator.current_size >= *rotator.RotateEveryBytes {
		return true
	}

	return false
}

func (rotator *FileRotator) FilePath(file_no int) string {
	if file_no == 0 {
		return filepath.Join(rotator.Path, rotator.Name)
	}
	filename := strings.Join([]string{rotator.Name, strconv.Itoa(file_no) + ".zip"}, ".")
	return filepath.Join(rotator.Path, filename)
}

func (rotator *FileRotator) FileExists(file_no int) bool {
	file_path := rotator.FilePath(file_no)
	_, err := os.Stat(file_path)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func (rotator *FileRotator) Rotate() error {
	if rotator.current != nil {
		if err := rotator.current.Close(); err != nil {
			return err
		}
	}
	if (rotator.current == nil) {
		// create the new file
		file_path := rotator.FilePath(0)
		current, err := os.Create(file_path)
		if err != nil {
			return err
		}
		rotator.current = current
		rotator.current_size = 0
		return nil
	}

	// create a new file based on the current timestamp
	timestamp := strconv.FormatInt(time.Now().UnixNano() / int64(time.Millisecond), 10)
	file_name := rotator.Name + "-" + timestamp + ".zip" ;
	zip_file_path := filepath.Join(rotator.Path, file_name)
	zipFile, cerr := os.Create(zip_file_path)
	if cerr != nil {
		return cerr
	}
	zipWriter := zip.NewWriter(zipFile)
	iow, zcerr := zipWriter.Create(rotator.Name)
	if zcerr != nil {
		return zcerr
	}

	// read the current log file and zip it up into the new zip file
	file, _ := os.Open(rotator.FilePath(0))
	fileReader := bufio.NewReader(file)
	blockSize := 512 * 1024 // 512kb
	bytes := make([]byte, blockSize)
	for {
		readedBytes, rerr := fileReader.Read(bytes)

		if rerr != nil {
			if rerr.Error() == "EOF" {
				break
			}

			if rerr.Error() != "EOF" {
				return rerr
			}
		}

		if readedBytes >= blockSize {
			iow.Write(bytes)
			continue
		}
		iow.Write(bytes[:readedBytes])
	}
	errr := zipWriter.Close()
	if errr != nil {
		return errr
	}
	file.Close()

	// create the new file
	file_path := rotator.FilePath(0)
	current, err := os.Create(file_path)
	if err != nil {
		return err
	}
	rotator.current = current
	rotator.current_size = 0

	for {
		// find all files
		filePathList := []string{}
		filepath.Walk(rotator.Path, func(path string, f os.FileInfo, err error) error {
			if (strings.HasSuffix(path, ".zip")) {
				filePathList = append(filePathList, path)
			}
			return nil
		})
		if (len(filePathList) < *rotator.KeepFiles) {
			break
		} else {
			sort.Strings(filePathList)
			os.Remove(filePathList[0])
		}
	}

	return nil
}