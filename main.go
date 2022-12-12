package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/jordan-wright/email"
	"log"
	"net/smtp"
	"net/url"
	"os"
	"soft2_importer/openAlex"
)

var rootPath = flag.String("oa", "data/openAlex/works", "the config file")
var importType = flag.String("type", "papers", "the import type, could be authors or papers")
var startDir = flag.String("sd", "", "the directory to start, if empty , start from newest directory")
var startFile = flag.String("sf", "", "the file to start, if empty , start from first file in directory")
var logDetail = flag.Bool("ld", true, "whether or not log detail")
var sendEmail = flag.Bool("se", false, "whether or not send email when error")
var startOffset = flag.Int64("so", 0, "start offset of the start file")
var logFile = flag.String("lf", "log.txt", "log file name")
var bulkNum = flag.Int("bn", 64, "one bulk paper num")
var logInternal = flag.Int("li", 5000, "log interval")

func main() {
	str := "我是一个带/的字符串"
	println(url.QueryEscape(url.QueryEscape(str)))
	println(url.QueryEscape(str))
	flag.Parse()
	defer func() {
		if *sendEmail {
			SendEmail()
		}
	}()
	logFile, err := os.Create(*logFile)
	openAlex.PanicError(err)
	log.SetOutput(logFile)
	log.Printf("totalpath : %s\n", *rootPath)
	log.Printf("startFile : %s\n", *startFile)
	log.Printf("bulkNum : %d\n", *bulkNum)
	log.Printf("logInterval : %d\n", *logInternal)
	openAlex.TotalPath = *rootPath
	openAlex.StartDir = *startDir
	openAlex.LogDetail = *logDetail
	log.Println("welcome to importer")
	if *importType == "authors" {
		openAlex.ImportScholars()
	} else if *importType == "papers" {
		log.Println("start to import papers")
		openAlex.GetPaperImporterContext(*rootPath, *startDir, *startFile, *startOffset, *bulkNum, *logInternal, *logDetail).Import()
	} else if *importType == "auto-complete" {
		log.Println("start to import auto completes")
		openAlex.GetAutoCompleteImporterContext(*rootPath, *startDir, *startFile, *startOffset, *bulkNum, *logInternal, *logDetail).Import()
	} else {
		openAlex.PanicError(errors.New("type argument is not authors or paper neither"))
	}
}

func SendEmail() {
	log.Printf("send email to 1838940019@qq.com\n")
	e := email.NewEmail()
	e.From = fmt.Sprintf("您的导入程序 <1838940019@qq.com>")
	//e.To = []string{"20373389@buaa.edu.cn"}
	e.To = []string{"1838940019@qq.com"}
	//设置文件发送的内容
	content := fmt.Sprintf(`您的程序又又又崩了， 请登陆华为云查看`)
	e.HTML = []byte(content)
	e.Subject = "您的程序又又又崩了"
	//设置服务器相关的配置
	//err := e.Send("smtp.qq.com:25", smtp.PlainAuth("", "413935740@qq.com", "ukdwwhkaegvpcbch", "smtp.qq.com"))
	err := e.Send("smtp.qq.com:25", smtp.PlainAuth("", "1838940019@qq.com", "gvlptmbocrkmfdgh", "smtp.qq.com"))
	openAlex.PanicError(err)
}

//func GetOAArticles() []*types.OAArticle {
//
//}
//func ImportPapers() {
//	log.Println("start import papers")
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
