package main

import (
	"soft2_importer/openAlex"
)

func main() {
	println("welcome to importer")
	openAlex.ImportScholars()
	//ImportPapers()
}

//func GetOAArticles() []*types.OAArticle {
//
//}
//func ImportPapers() {
//	print("start import papers")
//	for {
//		originPapers := GetOAArticles()
//		if len(originPapers) == 0 {
//			break
//		}
//		targetPapers := make([]*types.Paper, len(originPapers))
//		for i, e := range originPapers {
//			targetPapers[i] = e.Parse()
//		}
//		importPaperToES(targetPapers)
//	}
//}
//func importPaperToES(targets []*types.Paper) {
//
//}
