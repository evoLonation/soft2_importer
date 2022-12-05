package openAlex

//
//import (
//	"bufio"
//	"bytes"
//	"encoding/json"
//	"fmt"
//	"io/ioutil"
//	"log"
//	"os"
//	"soft2_importer/common"
//	"soft2_importer/types"
//	"sort"
//	"strconv"
//	"strings"
//	"time"
//)
//
//var paperCreateMeta = "{ \"create\" : { \"_index\" : \"papers\", \"_id\" : \"%s\"} }\n"
//var authorPubUpdateMeta = "{ \"update\" : { \"_index\" : \"authors\", \"_id\" : \"%s\"} }\n"
//var authorPubUpdateQuery = "{ \"scripted_upsert\": true, \"script\": { \"source\": \"if (!ctx._source.pubs.contains(params.pub)) {ctx._source.pubs.add(params.pub)}\", \"params\" : {\"pub\" : {\"i\" : \"%s\", \"r\" : %d}}}, \"upsert\": {\"pubs\" : []}}"
//
//type UpdateBulkResponse struct {
//	// 花了多长时间，milliseconds
//	Took int64 `json:"took"`
//	//是否存在出错
//	Errors bool `json:"errors"`
//	Items  []struct {
//		Update struct {
//			itemResponse
//		} `json:"update"`
//	} `json:"items"`
//}
//
//type ImporterContext[S Parseable[T], T ValidationAble] struct {
//	typ              string
//	originStructName string
//	relativePath     string
//	directoryPrefix  string
//	loadTime         int64
//	oneBulkQueryNum  int
//	lineLength       int
//	//总共多少条json
//	totalNum int64
//}
//type Parseable[T any] interface {
//	Parse() *T
//}
//type ValidationAble interface {
//	CheckValidation() bool
//}
//
//type AuthorImporterContext struct {
//	ImporterContext[OAScholar, types.Scholar]
//}
//
//type PaperImporterContext struct {
//}
//
//func (p *ImporterContext[S, T]) getOriginStruct(scanner *bufio.Scanner) []*S {
//	fmt.Printf("load %s structs......\n", p.originStructName)
//	defer fmt.Printf("load %s structs done\n", p.originStructName)
//	origins := make([]*S, p.oneBulkQueryNum)
//	//读取一行
//	i := 0
//	for scanner.Scan() {
//		line := scanner.Bytes()
//		origin := new(S)
//		err := json.Unmarshal(line, origin)
//		if err != nil {
//			log.Panicf("\nUnmarshal %d'st string to %s error : "+err.Error()+"\nthe string is %s", i+1, p.originStructName, string(line))
//		}
//		origins[i] = origin
//		i++
//		if i >= p.oneBulkQueryNum {
//			break
//		}
//	}
//	return origins[:i]
//}
//
//func (p *ImporterContext[S, T]) createScanner() *bufio.Scanner {
//	fmt.Printf("load file to create scanner\n")
//	defer fmt.Printf("create scanner done\n")
//	err := os.Chdir(common.TotalPath)
//	PanicError(err)
//	err = os.Chdir(p.relativePath)
//	PanicError(err)
//	fileInfos, err := ioutil.ReadDir(".")
//	PanicError(err)
//	var dirs []string
//	for _, info := range fileInfos {
//		if info.IsDir() && strings.Contains(info.Name(), p.directoryPrefix) {
//			dirs = append(dirs, info.Name())
//		}
//	}
//	sort.Strings(dirs)
//
//	var gzfiles []*os.File
//	for i := len(dirs) - 1; i >= 0; i-- {
//		subfileInfos, err := ioutil.ReadDir(dirs[i])
//		if err != nil {
//			log.Panic(err)
//		}
//		for _, fileinfo := range subfileInfos {
//			file, err := os.Open(dirs[i] + "/" + fileinfo.Name())
//			PanicError(err)
//			gzfiles = append(gzfiles, file)
//		}
//	}
//	reader := MultiFileReaderFactory(gzfiles)
//	scanner := bufio.NewScanner(reader)
//	buf := make([]byte, p.lineLength)
//	scanner.Buffer(buf, p.lineLength)
//	return scanner
//}
//
//func (p *ImporterContext[S, T]) ImportObjects() {
//	start := time.Now()
//	fmt.Printf("start to import %s to es!\n", p.typ)
//	defer func() {
//		fmt.Printf("done import, no error! \ntotal number is %d\n time: from %s to %s, duration %s", p.totalNum, start, time.Now(), time.Since(start))
//	}()
//	scanner := p.createScanner()
//	for {
//		fmt.Printf("%d'st iteration...\n", p.loadTime+1)
//		origins := p.getOriginStruct(scanner)
//		if len(origins) == 0 {
//			break
//		}
//		targets := make([]*T, len(origins))
//		for i, e := range origins {
//			targets[i] = e.Parse()
//		}
//		importScholarToES(targets)
//		fmt.Printf("%d'st iteration done!\n", loadTime)
//		loadTime++
//		totalNum += int64(len(origins))
//	}
//}
//
//func importScholarToES(targets []*types.Scholar) {
//	fmt.Printf("send created bulk request to ES...\n")
//	//对于每个targets，先判断是否有效，有效就创建
//	buffer := bytes.Buffer{}
//	for _, target := range targets {
//		if !target.CheckValidation() {
//			continue
//		}
//		meta := []byte(fmt.Sprintf(authorCreateMeta, target.Id, "\n"))
//		data, err := json.Marshal(target)
//		if err != nil {
//			log.Panic("marshal struct to string error: \n", err.Error())
//		}
//		data = append(data, "\n"...)
//		buffer.Grow(len(meta) + len(data))
//		buffer.Write(meta)
//		buffer.Write(data)
//	}
//	res, err := es.Bulk(bytes.NewReader(buffer.Bytes()))
//	if err != nil {
//		log.Panic("execute es.Bulk occurs error: \n", err.Error())
//	}
//	common.HandleResponseError(res)
//	block := CreatedBulkResponse{}
//	if err := json.NewDecoder(res.Body).Decode(&block); err != nil {
//		log.Panic("parse response body error:\n", err)
//	} else {
//		if block.Errors {
//			for _, item := range block.Items {
//				status := item.Create.Status
//				if status != 409 && status != 201 {
//					log.Panic("es internal error:\n", strconv.Itoa(item.Create.Status), item.Create.Error.Type, item.Create.Error.Reason)
//				}
//			}
//		}
//	}
//	fmt.Printf("send done\n")
//}
