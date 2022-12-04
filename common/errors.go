package common

import (
	"encoding/json"
	"flag"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"log"
)

var TotalPath = *flag.String("oa", "/home/diamond/soft2/data/openalex", "the config file")

//type errorRecorder struct {
//	NowIndex    int64
//	NowLocation string
//	StartId     string
//	EndId       string
//}
//
//var ErrorRecorder errorRecorder
//
//func (p *errorRecorder) Fatal(when string, otherInfos ...string) {
//	err := fmt.Sprintf("Failure!\nwhen: %s\n where: %s\nindex: %d\n", when, p.NowLocation, p.NowIndex)
//	if len(otherInfos) != 0 {
//		err += "otherInfos:\n"
//	}
//	for _, info := range otherInfos {
//		err += info + "\n"
//	}
//	log.Fatal(err)
//}
//
func HandleResponseError(res *esapi.Response) {
	raw := map[string]interface{}{}
	if res.IsError() {
		str := "http from ES responses error! \n"
		if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
			log.Fatal(str, "parse error response body error\n", err.Error())
		} else {
			log.Fatal("ES response Errors:\n",
				raw["error"].(map[string]interface{})["type"].(string), "\n",
				raw["error"].(map[string]interface{})["reason"].(string),
			)
		}
	}
}
