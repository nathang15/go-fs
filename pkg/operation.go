package pkg

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

type Operation struct{}

func (op *Operation) LocalFileExist(fileName string) bool {
	files, err := os.ReadDir("../localdb")
	if err != nil {
		log.Println("Could not open folder. Error: ", err)
		return true
	}

	for _, file := range files {
		if file.Name() == fileName {
			return true
		}
	}
	return false
}

func (op *Operation) ClearDB() {
	files, err := os.ReadDir("../localdb")
	if err != nil {
		log.Println("Could not open folder. Error: ", err)
		return
	}

	for _, file := range files {
		err := os.Remove(op.FilePath(file.Name()))
		if err != nil {
			log.Println("Could not delete file. Error: ", err)
		}
	}
}

func (op *Operation) ClearLocalDB(prefix string) {
	files, err := os.ReadDir("../localdb")
	if err != nil {
		log.Println("Could not open folder. Error: ", err)
		return
	}

	for _, file := range files {
		if !strings.HasPrefix(file.Name(), prefix) {
			continue
		}
		err := os.Remove(op.LocalFilePath(file.Name()))
		if err != nil {
			log.Printf("Could not delete file %s due to err %s \n", file.Name(), err)
		}
	}
}

func (op *Operation) Copy(src string, dest string) (int64, error) {
	srcPath := src

	srcFileStat, err := os.Stat(srcPath)
	if err != nil {
		return 0, err
	}

	if !srcFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", srcPath)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}

	defer source.Close()

	destPath := dest

	destination, err := os.Create(destPath)
	if err != nil {
		return 0, err
	}

	defer destination.Close()

	nBytes, err := io.Copy(destination, source)
	return nBytes, err
}

func (op *Operation) OpenFile(localFile string) *os.File {
	filepath := localFile
	file, err := os.Open(filepath)
	if err != nil {
		log.Println("Could not open file. Error: ", err)
		return nil
	}
	return file
}

// Open or create a file for writing
func (*Operation) CreateOrOpenFile(localfile string) *os.File {
	// check if file exists
	filepath := localfile

	// check if file exists
	if _, err := os.Stat(filepath); err == nil {
		deleteErr := os.Remove(filepath)
		if deleteErr != nil {
			log.Printf("Error: Unable to delete exisitng file %s. Error %s \n", filepath, deleteErr)
			return nil
		}
	}

	// open or create a new log file with preconfigured file name
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Error: Unable to create or open file %s. Error %s\n", filepath, err)
	}

	return file
}

func (*Operation) ListFilesInFolder(path string) []string {
	files, err := os.ReadDir(path)
	if err != nil {
		log.Fatal(err.Error())
		return nil
	}

	var fileNames []string
	for _, file := range files {
		if file.Name() == "desktop.ini" {
			continue
		}
		fileNames = append(fileNames, file.Name())
	}
	return fileNames
}

func (*Operation) Delete(fileName string) {
	err := os.Remove(fileName)
	if err != nil {
		log.Println("Could not delete file. Error: ", err)
		return
	}

	log.Printf("Deleted file %s \n", fileName)
}

func (*Operation) FilePath(fileName string) string {
	return fmt.Sprintf("%s/%s", config.FilePath, fileName)
}

func (op *Operation) LocalFilePath(fileName string) string {
	return fmt.Sprintf("%s/%s", config.LocalFilePath, fileName)
}
