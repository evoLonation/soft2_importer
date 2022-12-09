package common

import (
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"log"
)

func HandleResponseError(res *esapi.Response) bool {
	raw := map[string]interface{}{}
	if res.IsError() {
		str := "http from ES responses error! \n"
		if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
			log.Panic(str, "parse error response body error\n", err.Error())
		} else {
			log.Printf("ES http response Errors:\nstatus:%s\n%s\n%s\n%s\n",
				res.Status(),
				raw["error"].(map[string]interface{})["type"].(string),
				raw["error"].(map[string]interface{})["reason"].(string),
			)
			return false
		}
	}
	return true
}
