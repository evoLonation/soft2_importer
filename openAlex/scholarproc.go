package openAlex

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"io/ioutil"
	"log"
	"os"
	"soft2_importer/common"
	"soft2_importer/types"
	"sort"
	"strconv"
	"strings"
	"time"
)

var authorCreateMeta = `{ "create" : { "_index" : "authors", "_id" : "%s"} }%s`

type itemResponse struct {
	ID     string `json:"_id"`
	Result string `json:"result"`
	Status int    `json:"status"`
	Error  struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error"`
}

type CreatedBulkResponse struct {
	// 花了多长时间，milliseconds
	Took int64 `json:"took"`
	//是否存在出错
	Errors bool `json:"errors"`
	Items  []struct {
		Create struct {
			itemResponse
		} `json:"create"`
	} `json:"items"`
}

var es *elasticsearch.Client

func init() {
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://127.0.0.1:9200",
		},
	}
	var err error
	es, err = elasticsearch.NewClient(cfg)
	PanicError(err)
	log.Println(es.Info())
}

//updated_date=2022-08-28
var TotalPath string
var StartDir string
var relativePath = "authors"
var directoryPrefix string = "updated_date="
var loadTime int64
var oneBulkNum int = 1024
var lineLength int = 1 << 18 // 256k
var totalNum int64
var logInterval int64 = 100000
var LogDetail bool = false

func getOriginScholars(scanner *bufio.Scanner) []*OAScholar {
	if LogDetail {
		log.Printf("load OAScholar structs......\n")
	}
	defer func() {
		if LogDetail {
			log.Printf("load OAScholar structs done\n")
		}
	}()
	origins := make([]*OAScholar, oneBulkNum)
	//读取一行
	i := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		origin := &OAScholar{}
		err := json.Unmarshal(line, origin)
		if err != nil {
			log.Panicf("\nUnmarshal %d'st string to OAScholar error : "+err.Error()+"\nthe string is %s", i+1, string(line))
		}
		origins[i] = origin
		i++
		if i >= oneBulkNum {
			break
		}
	}
	return origins[:i]
}

func PanicError(err error) {
	if err != nil {
		panic(err)
	}
}
func createScanner() *bufio.Scanner {
	log.Printf("load file to create scanner\n")
	defer log.Printf("create scanner done\n")
	err := os.Chdir(TotalPath)
	PanicError(err)
	err = os.Chdir(relativePath)
	PanicError(err)
	fileInfos, err := ioutil.ReadDir(".")
	PanicError(err)
	var dirs []string
	for _, info := range fileInfos {
		if info.IsDir() && strings.Contains(info.Name(), directoryPrefix) {
			dirs = append(dirs, info.Name())
		}
	}
	sort.Strings(dirs)
	if StartDir != "" {
		log.Printf("start from directory %s\n", StartDir)
		i := len(dirs) - 1
		for ; i >= 0; i-- {
			if dirs[i] == StartDir {
				dirs = dirs[:i+1]
				break
			}
		}
		if i < 0 {
			log.Panicf("error: can not find start directory %s\n", StartDir)
		}
	}
	var files []string
	for i := len(dirs) - 1; i >= 0; i-- {
		subfileInfos, err := ioutil.ReadDir(dirs[i])
		if err != nil {
			log.Panic(err)
		}
		for _, fileinfo := range subfileInfos {
			if strings.HasSuffix(fileinfo.Name(), ".gz") {
				files = append(files, dirs[i]+"/"+fileinfo.Name())
			}
		}
	}
	reader := MultiFileReaderFactory(files)
	scanner := bufio.NewScanner(reader)
	buf := make([]byte, lineLength)
	scanner.Buffer(buf, lineLength)
	return scanner
}

func ImportScholars() {
	start := time.Now()
	log.Printf("start to import scholars to es!\n")
	defer func() {
		log.Printf("quit import, \ntotal number is %d\n time: from %s to %s, duration %s", totalNum, start, time.Now(), time.Since(start))
	}()
	scanner := createScanner()
	nextLogNum := logInterval
	for {
		if LogDetail {
			log.Printf("%d'st iteration...\n", loadTime)
		}
		originScholars := getOriginScholars(scanner)
		if len(originScholars) == 0 {
			break
		}
		targetScholars := make([]*types.Scholar, len(originScholars))
		for i, e := range originScholars {
			targetScholars[i] = e.Parse()
		}
		importScholarToES(targetScholars)
		if LogDetail {
			log.Printf("%d'st iteration done!\n", loadTime)
		}
		loadTime++
		totalNum += int64(len(originScholars))
		for totalNum > nextLogNum {
			log.Printf("already import %d lines, send %d bulk requests...\n", totalNum, loadTime)
			nextLogNum += logInterval
		}
	}
}

func importScholarToES(targets []*types.Scholar) {
	if LogDetail {
		log.Printf("send created bulk request to ES...\n")
	}
	//对于每个targets，先判断是否有效，有效就创建
	buffer := bytes.Buffer{}
	for _, target := range targets {
		if !target.CheckValidation() {
			continue
		}
		meta := []byte(fmt.Sprintf(authorCreateMeta, target.Id, "\n"))
		data, err := json.Marshal(target)
		if err != nil {
			log.Panic("marshal struct to string error: \n", err.Error())
		}
		data = append(data, "\n"...)
		buffer.Grow(len(meta) + len(data))
		buffer.Write(meta)
		buffer.Write(data)
	}
	res, err := es.Bulk(bytes.NewReader(buffer.Bytes()))
	if err != nil {
		log.Panic("execute es.Bulk occurs error: \n", err.Error())
	}
	common.HandleResponseError(res)
	block := CreatedBulkResponse{}
	if err := json.NewDecoder(res.Body).Decode(&block); err != nil {
		log.Panic("parse response body error:\n", err)
	} else {
		if block.Errors {
			for _, item := range block.Items {
				status := item.Create.Status
				if status != 409 && status != 201 {
					log.Panic("es internal error:\n", strconv.Itoa(item.Create.Status), item.Create.Error.Type, item.Create.Error.Reason)
				}
			}
		}
	}
	if LogDetail {
		log.Printf("send done\n")
	}
}
