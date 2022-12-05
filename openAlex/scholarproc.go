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
	es, _ = elasticsearch.NewClient(cfg)
	log.Println(es.Info())
}

//updated_date=2022-08-28
var TotalPath string
var relativePath = "authors"
var directoryPrefix string = "updated_date="
var loadTime int64
var oneBulkNum int = 1024
var lineLength int = 1 << 18 // 256k
var totalNum int64

func getOriginScholars(scanner *bufio.Scanner) []*OAScholar {
	fmt.Printf("load OAScholar structs......\n")
	defer fmt.Printf("load OAScholar structs done\n")
	origins := make([]*OAScholar, oneBulkNum)
	//读取一行
	i := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		origin := &OAScholar{}
		err := json.Unmarshal(line, origin)
		if err != nil {
			log.Fatalf("\nUnmarshal %d'st string to OAScholar error : "+err.Error()+"\nthe string is %s", i+1, string(line))
		}
		origins[i] = origin
		i++
		if i >= oneBulkNum {
			break
		}
	}
	return origins[:i]
}

func FatalError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
func createScanner() *bufio.Scanner {
	fmt.Printf("load file to create scanner\n")
	defer fmt.Printf("create scanner done\n")
	err := os.Chdir(TotalPath)
	FatalError(err)
	err = os.Chdir(relativePath)
	FatalError(err)
	fileInfos, err := ioutil.ReadDir(".")
	FatalError(err)
	var dirs []string
	for _, info := range fileInfos {
		if info.IsDir() && strings.Contains(info.Name(), directoryPrefix) {
			dirs = append(dirs, info.Name())
		}
	}
	sort.Strings(dirs)

	var files []string
	for i := len(dirs) - 1; i >= 0; i-- {
		subfileInfos, err := ioutil.ReadDir(dirs[i])
		if err != nil {
			log.Fatal(err)
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
	fmt.Printf("start to import scholars to es!\n")
	defer func() {
		fmt.Printf("done import, no error! \ntotal number is %d\n time: from %s to %s, duration %s", totalNum, start, time.Now(), time.Since(start))
	}()
	scanner := createScanner()
	for {
		fmt.Printf("%d'st iteration...\n", loadTime)
		originScholars := getOriginScholars(scanner)
		if len(originScholars) == 0 {
			break
		}
		targetScholars := make([]*types.Scholar, len(originScholars))
		for i, e := range originScholars {
			targetScholars[i] = e.Parse()
		}
		importScholarToES(targetScholars)
		fmt.Printf("%d'st iteration done!\n", loadTime)
		loadTime++
		totalNum += int64(len(originScholars))
	}
}

func importScholarToES(targets []*types.Scholar) {
	fmt.Printf("send created bulk request to ES...\n")
	//对于每个targets，先判断是否有效，有效就创建
	buffer := bytes.Buffer{}
	for _, target := range targets {
		if !target.CheckValidation() {
			continue
		}
		meta := []byte(fmt.Sprintf(authorCreateMeta, target.Id, "\n"))
		data, err := json.Marshal(target)
		if err != nil {
			log.Fatal("marshal struct to string error: \n", err.Error())
		}
		data = append(data, "\n"...)
		buffer.Grow(len(meta) + len(data))
		buffer.Write(meta)
		buffer.Write(data)
	}
	res, err := es.Bulk(bytes.NewReader(buffer.Bytes()))
	if err != nil {
		log.Fatal("execute es.Bulk occurs error: \n", err.Error())
	}
	common.HandleResponseError(res)
	block := CreatedBulkResponse{}
	if err := json.NewDecoder(res.Body).Decode(&block); err != nil {
		log.Fatal("parse response body error:\n", err)
	} else {
		if block.Errors {
			for _, item := range block.Items {
				status := item.Create.Status
				if status != 409 && status != 201 {
					log.Fatal("es internal error:\n", strconv.Itoa(item.Create.Status), item.Create.Error.Type, item.Create.Error.Reason)
				}
			}
		}
	}
	fmt.Printf("send done\n")
}
