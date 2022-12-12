package openAlex

import (
	"bytes"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"log"
	"math"
	"soft2_importer/common"
	"soft2_importer/types"
	"strings"
)

func GetAutoCompleteImporterContext(rootPath string, startDir string, startFile string, fileOffset int64, oneBulkNum int, logInterval int, logDetail bool) *PaperImporterContext {
	return &PaperImporterContext{
		ImporterContext: getImporterContext[OAArticle, *OAArticle, *types.Paper](rootPath, startDir, startFile, fileOffset, oneBulkNum, logInterval, logDetail, importAutoPaperToES),
	}
}

var autoUpdateQuery = `
{
    "scripted_upsert": true,
    "script": {
        "source": "ctx._source.hot_word.weight += %d"
    },
    "upsert": {
        "hot_word": {
            "input" : "%s",
            "weight" : %d
        }
    }
}`

func importAutoPaperToES(targets []*types.Paper, logDetail bool, createdNumChan chan int) {
	success := false
	defer func() {
		if !success {
			createdNumChan <- -1
		}
	}()
	if logDetail {
		log.Printf("send auto complete paper bulk request to ES...\n")
	}
	if len(targets) == 0 {
		log.Printf("targets length = 0, why? anyway i dont send request")
		success = true
		createdNumChan <- 0
		return
	}
	totalCreatedNum := 0
	for _, target := range targets {
		if target.NCitation < 1000 {
			if logDetail {
				log.Printf("paper number of citation is small than 1000! ")
			}
			continue
		}
		//log.Printf("find a paper that number of citation is bigger than 1000!\n")
		// 计算得到id和query
		var ids []string
		var querys []string
		ids = append(ids, removeUnavailableCharacter(target.Title))
		weight := int(math.Min(10000, 1+(float64(target.NCitation)-1000)*0.1))
		querys = append(querys, fmt.Sprintf(autoUpdateQuery, weight, removeUnavailableCharacter(target.Title), weight))
		for _, e := range target.Keywords {
			ids = append(ids, removeUnavailableCharacter(e))
			querys = append(querys, fmt.Sprintf(autoUpdateQuery, weight/len(target.Keywords), removeUnavailableCharacter(e), weight/len(target.Keywords)))
			//log.Printf("id : %s", removeUnavailableCharacter(e))
			//log.Printf("query: %s", fmt.Sprintf(autoUpdateQuery, weight/len(target.Keywords), removeUnavailableCharacter(e), weight/len(target.Keywords)))
		}
		for i := 0; i < len(ids); i++ {
			for tryTime := 0; ; tryTime++ {
				if tryTime >= 3 {
					log.Panic("try 3 times to recovery the es error, but failed\n")
				}
				res, err := es.Update("auto-complete", ids[i], bytes.NewBufferString(querys[i]))
				createdNum := checkAutoSuccess(err, res)
				if createdNum == -1 {
					checkESReadyRetry()
				} else {
					totalCreatedNum += createdNum
					break
				}
			}
		}
	}
	success = true
	createdNumChan <- totalCreatedNum
	return
}

func checkAutoSuccess(err error, res *esapi.Response) int {
	common.HandleResponseError(res)
	if err != nil {
		log.Printf("execute es.Bulk occurs error: %s\n", err.Error())
		return -1
	}
	if res.StatusCode == 201 {
		return 1
	} else if res.StatusCode == 200 {
		return 0
	} else {
		log.Printf("http response neither 200 or 201\n")
	}
	return -1
}

func removeUnavailableCharacter(str string) string {
	str = strings.ReplaceAll(str, "\\u0000", "")
	str = strings.ReplaceAll(str, "\\u001f", "")
	str = strings.ReplaceAll(str, "\\u001e", "")
	return str
}
