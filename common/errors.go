package common

import (
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"log"
)

func HandleResponseError(res *esapi.Response) {
	raw := map[string]interface{}{}
	if res.IsError() {
		str := "http from ES responses error! \n"
		if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
			log.Panic(str, "parse error response body error\n", err.Error())
		} else {
			log.Panic("ES response Errors:\n",
				raw["error"].(map[string]interface{})["type"].(string), "\n",
				raw["error"].(map[string]interface{})["reason"].(string),
			)
		}
	}
}
