package openAlex

import (
	"fmt"
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
}

func MultiFileReaderFactory(gzfiles []string) *multiFileReader {
	fmt.Printf("build a multi gz file reader, gz files : \n")
	for _, file := range gzfiles {
		println(file)
	}
	firstJsonlname := gunzip(gzfiles[0])
	firstJsonFile, err := os.Open(firstJsonlname)
	FatalError(err)
	return &multiFileReader{
		gzfiles:          gzfiles,
		currentJsonlFile: firstJsonFile,
	}
}
func gunzip(file string) string {
	if !strings.HasSuffix(file, ".gz") {
		log.Fatalf("该文件不是.gz文件 : %s", file)
	}
	fmt.Printf("gunzip file %s...", file)
	cmd := exec.Command("/bin/bash", "-c", `gunzip -k -f `+file)
	output, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal("无法获取ungzip的标准输出管道", err.Error())
	}
	if err := cmd.Start(); err != nil {
		log.Fatal("gunzip命令执行失败，请检查命令输入是否有误", err.Error())
	}
	bytes, err := ioutil.ReadAll(output)
	FatalError(err)
	FatalError(cmd.Wait())
	fmt.Printf("unzip done\n")
	fmt.Printf("命令输出：\n%s\n", string(bytes))
	before, _, _ := strings.Cut(file, ".gz")
	return before
}

func (p *multiFileReader) Read(buf []byte) (int, error) {
	totaln := 0
	if p.currentIndex >= len(p.gzfiles) {
		fmt.Printf("all file are read done\n")
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
			log.Fatalf("read file %s error!\n %s\n", p.currentJsonlFile.Name(), err.Error())
		}
		//fmt.Printf("read %d bytes into [%d-%d) from file %s\n", tmp, totaln, totaln+tmp, p.currentJsonlFile.Name())
		totaln += tmp
		if totaln == len(buf) {
			break
		} else if totaln > len(buf) {
			log.Fatal("totaln should not greater than %s\n", len(buf))
		} else if totaln < len(buf) {
			fmt.Printf("the file %s are read done\n", p.currentJsonlFile.Name())
			err := p.currentJsonlFile.Close()
			FatalError(err)
			err = os.Remove(p.currentJsonlFile.Name())
			FatalError(err)
			p.currentIndex++
			if p.currentIndex >= len(p.gzfiles) {
				fmt.Printf("all file are read done\n")
				return totaln, nil
			}
			jsonlname := gunzip(p.gzfiles[p.currentIndex])
			p.currentJsonlFile, err = os.Open(jsonlname)
			FatalError(err)
			fmt.Printf("swap to next file : %s\n", p.currentJsonlFile.Name())
		}
	}
	return totaln, nil
}
