package openAlex

import (
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
)

type multiFileReader struct {
	currentIndex     int
	gzfiles          []string
	currentJsonlFile *os.File
	currentOffset    int64
}

var isZip bool = true

func MultiFileReaderFactory(gzfiles []string) *multiFileReader {
	log.Printf("build a multi gz file reader, gz files : \n")
	for _, file := range gzfiles {
		log.Println(file)
	}
	firstJsonlname := gunzip(gzfiles[0])
	firstJsonFile, err := os.Open(firstJsonlname)
	PanicError(err)
	return &multiFileReader{
		gzfiles:          gzfiles,
		currentJsonlFile: firstJsonFile,
	}
}
func gunzip(file string) string {
	if !isZip {
		return file
	}
	if !strings.HasSuffix(file, ".gz") {
		log.Panicf("该文件不是.gz文件 : %s", file)
	}
	log.Printf("gunzip file %s...", file)
	cmd := exec.Command("/bin/bash", "-c", `gunzip -k -f `+file)
	output, err := cmd.StdoutPipe()
	if err != nil {
		log.Panic("无法获取 ungzip 的标准输出管道", err.Error())
	}
	if err := cmd.Start(); err != nil {
		log.Panic("gunzip命令执行失败，请检查命令输入是否有误", err.Error())
	}
	bytes, err := ioutil.ReadAll(output)
	PanicError(err)
	PanicError(cmd.Wait())
	log.Printf("unzip done\n")
	log.Printf("gunzip command output :%s\n", string(bytes))
	before, _, _ := strings.Cut(file, ".gz")
	return before
}
func (p *multiFileReader) GetCurrentFileOffset() int64 {
	return p.currentOffset
}
func (p *multiFileReader) GetCurrentFile() string {
	if p.IsAllDone() {
		return "no file"
	} else {
		return p.gzfiles[p.currentIndex]
	}
}
func (p *multiFileReader) IsAllDone() bool {
	return p.currentIndex >= len(p.gzfiles)
}
func (p *multiFileReader) Read(buf []byte) (int, error) {
	totaln := 0
	if p.currentIndex >= len(p.gzfiles) {
		log.Printf("all file are read done\n")
		return totaln, nil
	}
	//fmt.Printf("read %d bytes from gzfiles! begin from file %s\n", len(buf), p.currentJsonlFile.Name())
	//defer fmt.Printf("read file done!\n")
	for {
		if p.currentIndex >= len(p.gzfiles) {
			return totaln, nil
		}
		tmp, err := p.currentJsonlFile.Read(buf[totaln:])
		if err != nil {
			log.Panicf("read file %s error!\n %s\n", p.currentJsonlFile.Name(), err.Error())
		}
		//fmt.Printf("read %d bytes into [%d-%d) from file %s\n", tmp, totaln, totaln+tmp, p.currentJsonlFile.Name())
		totaln += tmp
		p.currentOffset += int64(tmp)
		if totaln == len(buf) {
			break
		} else if totaln > len(buf) {
			log.Panicf("totaln should not greater than %d\n", len(buf))
		} else if totaln < len(buf) {
			log.Printf("the file %s are read done\n", p.currentJsonlFile.Name())
			err := p.currentJsonlFile.Close()
			PanicError(err)
			if isZip {
				err = os.Remove(p.currentJsonlFile.Name())
				PanicError(err)
			}
			p.currentIndex++
			p.currentOffset = 0
			if p.currentIndex >= len(p.gzfiles) {
				log.Printf("all file are read done\n")
				return totaln, nil
			}
			jsonlname := gunzip(p.gzfiles[p.currentIndex])
			p.currentJsonlFile, err = os.Open(jsonlname)
			PanicError(err)
			log.Printf("swap to next file : %s\n", p.currentJsonlFile.Name())
		}
	}
	return totaln, nil
}
