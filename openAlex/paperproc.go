package openAlex

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"reflect"
	"soft2_importer/common"
	"soft2_importer/types"
	"sort"
	"strings"
	"time"
)

type UpdateBulkResponse struct {
	// 花了多长时间，milliseconds
	Took int64 `json:"took"`
	//是否存在出错
	Errors bool `json:"errors"`
	Items  []struct {
		Update struct {
			itemResponse
		} `json:"update"`
		Create struct {
			itemResponse
		} `json:"create"`
	} `json:"items"`
}

type Parseable[T any] interface {
	Parse() T
}

type ValidationAble interface {
	CheckValidation() bool
}

type ImporterContext[SS any, SP Parseable[TP], TP ValidationAble] struct {
	rootPath        string
	startDir        string
	startFile       string
	startOffset     int64
	directoryPrefix string
	oneBulkNum      int
	lineLength      int
	logInterval     int
	logDetail       bool
	sourceTypeName  string
	targetTypeName  string
	target          string
	createdNum      int
	importToES      func(target []TP, logDetail bool) int
	fileReader      *multiFileReader
}

type PaperImporterContext struct {
	*ImporterContext[OAArticle, *OAArticle, *types.Paper]
}

func GetPaperImporterContext(rootPath string, startDir string, startFile string, fileOffset int64, oneBulkNum int, logDetail bool) *PaperImporterContext {
	return &PaperImporterContext{
		ImporterContext: getImporterContext[OAArticle, *OAArticle, *types.Paper](rootPath, startDir, startFile, fileOffset, oneBulkNum, logDetail, importPaperToES),
	}
}
func getImporterContext[SS any, SP Parseable[TP], TP ValidationAble](
	rootPath string,
	startDir string,
	startFile string,
	startOffset int64,
	oneBulkNum int,
	logDetail bool,
	importFunc func(target []TP, logDetail bool) int,
) *ImporterContext[SS, SP, TP] {
	try := any(new(SS))
	_, is := try.(SP)
	if !is {
		log.Panic("error when build importer context: type param SS's pointer is not parseable")
	}
	return &ImporterContext[SS, SP, TP]{
		rootPath:        rootPath,
		startDir:        startDir,
		startFile:       startFile,
		startOffset:     startOffset,
		directoryPrefix: "updated_date=",
		oneBulkNum:      oneBulkNum,
		lineLength:      1 << 22, // 4M
		logInterval:     5000,
		logDetail:       logDetail,
		sourceTypeName:  reflect.TypeOf(new(SP)).Name(),
		targetTypeName:  reflect.TypeOf(new(TP)).Name(),
		importToES:      importFunc,
	}
}

func (p *ImporterContext[SS, SP, TP]) getSourceStructs(scanner *bufio.Scanner) []SP {
	if p.logDetail {
		log.Printf("load %s structs......\n", p.sourceTypeName)
	}

	origins := make([]SP, p.oneBulkNum)
	//读取一行
	i := 0
	defer func() {
		if p.logDetail {
			log.Printf("load %s structs done, %d elements\n", p.sourceTypeName, i)
		}
	}()
	for scanner.Scan() {
		line := scanner.Bytes()
		origin := new(SS)
		err := json.Unmarshal(line, origin)
		if err != nil {
			log.Panicf("\nUnmarshal %d'st string to OAScholar error : %s\nthe string is %s\n", i+1, err.Error(), string(line))
		}
		origins[i] = (any(origin)).(SP)
		i++
		if i >= p.oneBulkNum {
			break
		}
	}
	return origins[:i]
}

func (p *ImporterContext[SS, SP, TP]) createScanner() *bufio.Scanner {
	log.Printf("load file to create scanner\n")
	defer log.Printf("create scanner done\n")
	err := os.Chdir(p.rootPath)
	PanicError(err)
	fileInfos, err := ioutil.ReadDir(".")
	PanicError(err)
	var dirs []string
	for _, info := range fileInfos {
		if info.IsDir() && strings.Contains(info.Name(), p.directoryPrefix) {
			dirs = append(dirs, info.Name())
		}
	}
	sort.Strings(dirs)
	//reverse array
	for i, j := 0, len(dirs)-1; i < j; i, j = i+1, j-1 {
		dirs[i], dirs[j] = dirs[j], dirs[i]
	}
	if p.startDir != "" {
		flag := false
		for i, dir := range dirs {
			if dir == p.startDir {
				dirs = dirs[i:]
				flag = true
				break
			}
		}
		if !flag {
			log.Panicf("error: can not find start directory %s\n", p.startDir)
		}
	}
	log.Printf("start from directory %s\n", dirs[0])

	var files []string
	for _, dir := range dirs {
		subfileInfos, err := ioutil.ReadDir(dir)
		PanicError(err)
		var subfiles []string
		for _, fileinfo := range subfileInfos {
			if strings.HasSuffix(fileinfo.Name(), ".gz") {
				subfiles = append(subfiles, dir+"/"+fileinfo.Name())
			}
		}
		sort.Strings(subfiles)
		files = append(files, subfiles...)
	}
	if p.startFile != "" {
		flag := false
		for i, file := range files {
			if strings.HasPrefix(file, dirs[0]) {
				if file == dirs[0]+"/"+p.startFile {
					files = files[i:]
					flag = true
					break
				}
			} else {
				break
			}
		}
		if !flag {
			log.Panicf("error: can not find start file %s in directory %s\n", p.startFile, p.startDir)
		}
	}
	log.Printf("start from file %s\n", files[0])
	reader := MultiFileReaderFactory(files)
	scanner := bufio.NewScanner(reader)
	p.fileReader = reader
	buf := make([]byte, p.lineLength)
	scanner.Buffer(buf, p.lineLength)
	// pass to startoffset
	if p.startOffset > 0 {
		log.Printf("pass this file to %d...\n", p.startOffset)
		for p.fileReader.currentOffset < p.startOffset-int64(p.lineLength) {
			scanner.Scan()
		}
		log.Printf("pass done, currentOffset: %d\n", p.fileReader.currentOffset)
	}
	return scanner
}

func (p *ImporterContext[SS, SP, TP]) Import() {
	start := time.Now()
	loadTime := 1
	totalNum := 0
	totalCreatedNum := 0
	log.Printf("start to import %s to es! start time : %s\n", p.target, start)
	defer func() {
		log.Printf("quit import, \ntotal number is %d\nactual create document number is %d\ntime: from %s to %s, duration %s\n", totalNum, totalCreatedNum, start, time.Now(), time.Since(start))
		if p.fileReader.IsAllDone() {
			log.Printf("conguratulations! all papers are import to your database!\n")
		} else {
			log.Printf("stop in file %s, offset %d, it is recommended to start from %d next time\n", p.fileReader.GetCurrentFile(), p.fileReader.GetCurrentFileOffset(), p.fileReader.GetCurrentFileOffset()-1*int64(p.lineLength)-1)
		}
	}()
	scanner := p.createScanner()
	nextLogNum := p.logInterval
	lastRestart := time.Now()
	for {
		if time.Since(lastRestart).Hours() >= 12 {
			log.Printf("already pass 12 hours util last restart\n")
			restartContainer()
			lastRestart = time.Now()
		}
		if p.logDetail {
			log.Printf("%d'st iteration...\n", loadTime)
		}
		sourceStructs := p.getSourceStructs(scanner)
		if len(sourceStructs) == 0 {
			break
		}
		targetStructs := make([]TP, len(sourceStructs))
		for i, e := range sourceStructs {
			targetStructs[i] = e.Parse()
		}
		totalCreatedNum += p.importToES(targetStructs, p.logDetail)
		if p.logDetail {
			log.Printf("%d'st iteration done!\n", loadTime)
		}
		loadTime++
		totalNum += len(sourceStructs)
		for totalNum > nextLogNum {
			log.Printf("already import %d lines and send %d bulk requests...\n", totalNum, loadTime)
			nextLogNum += p.logInterval
		}
	}
}

var paperCreateMeta = "{ \"create\" : { \"_index\" : \"papers\", \"_id\" : \"%s\"} }\n"
var authorPubUpdateMeta = "{ \"update\" : { \"_index\" : \"authors\", \"_id\" : \"%s\"} }\n"
var authorPubUpdateQuery = "{ \"scripted_upsert\": true, \"script\": { \"source\": \"if (!ctx._source.pubs.contains(params.pub)) {ctx._source.pubs.add(params.pub)}\", \"params\" : {\"pub\" : {\"i\" : \"%s\", \"r\" : %d}}}, \"upsert\": %s}\n"

func restartContainer() {
	log.Printf("restart elasticsearch container ...")
	cmd := exec.Command("/bin/bash", "-c", "docker container restart es")
	output, err := cmd.StdoutPipe()
	if err != nil {
		log.Panic("无法获取 docker restart 命令 的标准输出管道", err.Error())
	}
	if err := cmd.Start(); err != nil {
		log.Panic("docker container restart 命令执行失败，请检查命令输入是否有误", err.Error())
	}
	cmdOutputStr, err := ioutil.ReadAll(output)
	PanicError(err)
	PanicError(cmd.Wait())
	log.Printf("restart done\n")
	log.Printf("docker restart command output :%s\n", string(cmdOutputStr))
	log.Printf("wait 60 second to prepare elasticsearch\n")
	time.Sleep(time.Minute)
	GetNewClient()
}

func checkESReadyRetry() {
	trytime := 0
	for {
		if checkESReady() {
			break
		} else {
			trytime++
			if trytime > 10 {
				log.Panic("retry 10 times\n")
			}
			log.Printf("es shards not ready yet. retry after 10 seconds...\n")
			time.Sleep(10 * time.Second)
		}
	}
}

func checkESReady() bool {
	log.Printf("check shard ready...\n")
	return getIndexCount("papers") && getIndexCount("authors")
}

func getIndexCount(indexname string) bool {
	res, err := es.Count(es.Count.WithIndex(indexname))
	if err != nil {
		log.Printf("send request error:\n%s", err.Error())
		return false
	}
	if !common.HandleResponseError(res) {
		return false
	}
	mp := map[string]interface{}{}
	err = json.NewDecoder(res.Body).Decode(&mp)
	PanicError(err)

	log.Printf("%s index count:%f\n", indexname, mp["count"].(float64))
	return true
}

func importPaperToES(targets []*types.Paper, logDetail bool) (createdNum int) {
	if logDetail {
		log.Printf("send update paper bulk request to ES...\n")
	}
	if len(targets) == 0 {
		log.Printf("targets length = 0, why? anyway i dont send request")
	}
	//对于每个targets，先判断是否有效，有效就创建
	buffer := bytes.Buffer{}
	validationNum := 0
	for _, target := range targets {
		if !target.CheckValidation() {
			if logDetail {
				unvalidationBytes, err := json.Marshal(target)
				PanicError(err)
				log.Printf("target unvalidation! struct content:\n%s", string(unvalidationBytes))
			}
			continue
		}
		validationNum++
		paperMeta := []byte(fmt.Sprintf(paperCreateMeta, target.Id))
		paperData, err := json.Marshal(target)
		if err != nil {
			log.Panicf("marshal struct to string error: \n%s", err.Error())
		}
		paperData = append(paperData, "\n"...)

		authorMetas := make([][]byte, len(target.Authors))
		authorDatas := make([][]byte, len(target.Authors))

		authorLen := 0
		for i, e := range target.Authors {
			authorMetas[i] = []byte(fmt.Sprintf(authorPubUpdateMeta, e.Id))
			upsertAuthor := &types.Scholar{
				Id:        e.Id,
				Name:      e.Name,
				NCitation: -1,
				NPubs:     -1,
				HIndex:    -1,
				Statistics: []struct {
					Year      int64 `json:"year"`
					NCitation int64 `json:"n_citation"`
					NPubs     int64 `json:"n_pubs"`
				}{},
				Pubs: []struct {
					Id    string `json:"i"`
					Order int64  `json:"r"`
				}{},
				Tags: []struct {
					Name   string `json:"t"`
					Weight int64  `json:"w"`
				}{},
			}
			if e.Org != "" {
				upsertAuthor.Orgs = []string{e.Org}
			} else {
				upsertAuthor.Orgs = []string{}
			}
			upsertAuthorData, err := json.Marshal(upsertAuthor)
			if err != nil {
				log.Panicf("marshal struct author to string error: \n%s", err.Error())
			}
			authorDatas[i] = []byte(fmt.Sprintf(authorPubUpdateQuery, target.Id, i, string(upsertAuthorData)))
			authorLen += len(authorMetas[i]) + len(authorDatas[i])
		}

		buffer.Grow(len(paperMeta) + len(paperData) + authorLen)
		buffer.Write(paperMeta)
		buffer.Write(paperData)
		for i, _ := range authorDatas {
			buffer.Write(authorMetas[i])
			buffer.Write(authorDatas[i])
		}
	}
	if buffer.String() == "" {
		if logDetail {
			log.Printf("all targets unvalidation, do not send request\n")
		}
		return 0
	}

	beforeString := string(buffer.Bytes())
	beforeString = strings.ReplaceAll(beforeString, "\\u0000", "")
	beforeString = strings.ReplaceAll(beforeString, "\\u001f", "")
	beforeString = strings.ReplaceAll(beforeString, "\\u001e", "")
	if logDetail {
		log.Printf("execute body: \n%s", string(buffer.Bytes()))
	}

	var res *esapi.Response
	var err error
	res, err = es.Bulk(bytes.NewReader([]byte(beforeString)))
	if err != nil || !common.HandleResponseError(res) {
		if err != nil {
			log.Printf("execute es.Bulk occurs error: %s\n", err.Error())
		}
		checkESReadyRetry()
		res, err = es.Bulk(bytes.NewReader([]byte(beforeString)))
		if err != nil || !common.HandleResponseError(res) {
			if err != nil {
				log.Panicf("execute es.Bulk occurs error: %s\n", err.Error())
			}
			log.Panic()
		}
	}

	block := UpdateBulkResponse{}
	if err := json.NewDecoder(res.Body).Decode(&block); err != nil {
		log.Panic("parse response body error:\n", err)
	} else {
		if block.Errors {
			for _, item := range block.Items {
				status := 0
				var errBody map[string]interface{}
				if item.Update.Status != 0 {
					status = item.Update.Status
					errBody = item.Update.Error
				} else if item.Create.Status != 0 {
					status = item.Create.Status
					errBody = item.Create.Error
					if status == 201 {
						createdNum++
					}
				} else {
					log.Panic("item mapping error\n")
				}
				if status != 409 && status != 201 && status != 200 {
					errBytes, err := json.Marshal(errBody)
					if err != nil {
						log.Panicf("error when marshal es response error : %s\n", err)
					}
					log.Panicf("es internal error:\nstatus : %d\nerror json : \n%s\n", item.Create.Status, string(errBytes))
				}

			}
		} else {
			createdNum += validationNum
		}
	}
	if logDetail {
		log.Printf("send done\n")
	}
	return
}
