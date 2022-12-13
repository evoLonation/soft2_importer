package openAlex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"log"
	"math"
	"net/url"
	"soft2_importer/common"
	"soft2_importer/types"
	"strings"
)

func GetAutoCompleteImporterContext() *AutoCompleteContext {
	return &AutoCompleteContext{
		oneRequestSize: 500,
		maxNumber:      10000,
	}
}

type AutoCompleteContext struct {
	oneRequestSize int
	maxNumber      int
}

var searchByCitationQuery = `
{
    "from": %d,
    "size": %d,
    "sort": {
        "n_citation": {
            "order": "desc"
        }
    },
    "fields": [
        "keywords",
        "title",
        "n_citation"
    ],
    "_source": false
}`

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

func (p *AutoCompleteContext) Import() {
	log.Printf("start import")
	totalCreatedNum := 0
	for i := 0; i < 10000; i += p.oneRequestSize {
		log.Printf("already pass %d items, created %d hot words", i, totalCreatedNum)
		from := i
		size := int(math.Min(float64(p.oneRequestSize), float64(p.maxNumber-i)))
		res, err := searchPaper(*bytes.NewBufferString(fmt.Sprintf(searchByCitationQuery, from, size)))
		PanicError(err)
		for _, hit := range res["hits"].(map[string]interface{})["hits"].([]interface{}) {
			fields := hit.(map[string]interface{})["fields"].(map[string]interface{})
			title := fields["title"].([]interface{})[0].(string)
			nCitation := fields["n_citation"].([]interface{})[0].(float64)
			keywords := fields["keywords"].([]interface{})

			var ids []string
			var querys []string
			ids = append(ids, removeUnavailableCharacter(url.QueryEscape(title)))
			weight := int(math.Min(10000, float64(nCitation)*0.1))
			querys = append(querys, fmt.Sprintf(autoUpdateQuery, weight, removeUnavailableCharacter(title), weight))
			for _, e := range keywords {
				keyword := e.(string)
				ids = append(ids, url.QueryEscape(removeUnavailableCharacter(keyword)))
				querys = append(querys, fmt.Sprintf(autoUpdateQuery, weight/len(keywords), removeUnavailableCharacter(keyword), weight/len(keywords)))
				//log.Printf("id : %s", removeUnavailableCharacter(e))
				//log.Printf("query: %s", fmt.Sprintf(searchByCitationQuery, weight/len(target.Keywords), removeUnavailableCharacter(e), weight/len(target.Keywords)))
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
	}
}

func searchPaper(query bytes.Buffer) (map[string]interface{}, error) {
	var res map[string]interface{}
	resp, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex("papers"),
		es.Search.WithBody(&query),
		es.Search.WithTrackTotalHits(true),
		es.Search.WithPretty(),
	)
	if err != nil {
		log.Printf("Error getting response: %s\n", err)
	}
	if resp.IsError() {
		raw := map[string]interface{}{}
		errStr := "http from ES responses error! \n"
		if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
			errStr += fmt.Sprintf("parse error response body error:\n%s", err.Error())
		} else {
			_, success := raw["error"].(map[string]interface{})
			if success {
				errStr += fmt.Sprintf("ES http response Errors:\nstatus:%s\n%s\n%s\n%s\n",
					resp.Status(),
					raw["error"].(map[string]interface{})["type"].(string),
					raw["error"].(map[string]interface{})["reason"].(string),
				)
			} else {
				errStr += fmt.Sprintf("ES http response Errors:\n%s", raw["error"])
			}
			return nil, errors.New(errStr)
		}
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, errors.New(fmt.Sprintf("Error parsing the response body: %s\n", err))
	}
	return res, nil
}

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
		ids = append(ids, removeUnavailableCharacter(url.QueryEscape(target.Title)))
		weight := int(math.Min(10000, 1+(float64(target.NCitation)-1000)*0.1))
		querys = append(querys, fmt.Sprintf(searchByCitationQuery, weight, removeUnavailableCharacter(target.Title), weight))
		for _, e := range target.Keywords {
			ids = append(ids, url.QueryEscape(removeUnavailableCharacter(e)))
			querys = append(querys, fmt.Sprintf(searchByCitationQuery, weight/len(target.Keywords), removeUnavailableCharacter(e), weight/len(target.Keywords)))
			//log.Printf("id : %s", removeUnavailableCharacter(e))
			//log.Printf("query: %s", fmt.Sprintf(searchByCitationQuery, weight/len(target.Keywords), removeUnavailableCharacter(e), weight/len(target.Keywords)))
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
		return 0
	}
}

func removeUnavailableCharacter(str string) string {
	str = strings.ReplaceAll(str, "\\u0000", "")
	str = strings.ReplaceAll(str, "\\u001f", "")
	str = strings.ReplaceAll(str, "\\u001e", "")
	return str
}
