package common

import (
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"log"
)

func HandleResponseError(res *esapi.Response) bool {
	raw := map[string]interface{}{}
	if res.IsError() {
		log.Printf("http from ES responses error! \n")
		if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
			log.Panic("parse error response body error\n", err.Error())
		} else {
			_, success := raw["error"].(map[string]interface{})
			if success {
				log.Printf("ES http response Errors:\nstatus:%s\n%s\n%s\n%s\n",
					res.Status(),
					raw["error"].(map[string]interface{})["type"].(string),
					raw["error"].(map[string]interface{})["reason"].(string),
				)
			} else {
				log.Printf("ES http response Errors:\n")
				log.Println(raw["error"])
			}
			return false
		}
	}
	return true
}
