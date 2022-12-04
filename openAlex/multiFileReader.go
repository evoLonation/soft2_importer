package openAlex

import (
	"fmt"
	"log"
	"os"
)

type multiFileReader struct {
	currentIndex int
	files        []*os.File
}

func MultiFileReaderFactory(files []*os.File) *multiFileReader {
	fmt.Printf("build a multi file reader, files : \n")
	for _, file := range files {
		println(file.Name())
	}
	return &multiFileReader{
		files: files,
	}
}
func (p *multiFileReader) getCurrentFile() *os.File {
	return p.files[p.currentIndex]
}
func (p *multiFileReader) Read(buf []byte) (int, error) {
	totaln := 0
	if p.currentIndex >= len(p.files) {
		return totaln, nil
	}
	fmt.Printf("read %d bytes from files! begin from file %s\n", len(buf), p.getCurrentFile().Name())
	defer fmt.Printf("read file done!\n")
	for {
		if p.currentIndex >= len(p.files) {
			return totaln, nil
		}
		tmp, err := p.getCurrentFile().Read(buf[totaln:])
		if err != nil {
			log.Fatalf("read file %s error!\n %s\n", p.getCurrentFile().Name(), err.Error())
		}
		fmt.Printf("read %d bytes into [%d-%d) from file %s\n", tmp, totaln, totaln+tmp, p.getCurrentFile().Name())
		totaln += tmp
		if totaln == len(buf) {
			break
		} else if totaln > len(buf) {
			log.Fatal("totaln should not greater than n\n")
		} else if totaln < len(buf) {
			p.currentIndex++

		}
	}
	return totaln, nil
}
