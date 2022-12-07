package common

import (
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"log"
)

func HandleResponseError(res *esapi.Response) string {
	raw := map[string]interface{}{}
	if res.IsError() {
		str := "http from ES responses error! \n"
		if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
			log.Panic(str, "parse error response body error\n", err.Error())
		} else {
			return fmt.Sprintf("ES response Errors:\n%s\n%s\n",
				raw["error"].(map[string]interface{})["type"].(string),
				raw["error"].(map[string]interface{})["reason"].(string),
			)
		}
	}
	return ""
}
