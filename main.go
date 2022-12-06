package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/jordan-wright/email"
	"log"
	"net/smtp"
	"os"
	"soft2_importer/openAlex"
)

var rootPath = flag.String("oa", "data/openAlex/works", "the config file")
var importType = flag.String("type", "papers", "the import type, could be authors or papers")
var startDir = flag.String("sd", "", "the directory to start, if empty , start from newest directory")
var startFile = flag.String("sf", "", "the file to start, if empty , start from first file in directory")
var logDetail = flag.Bool("ld", true, "whether or not log detail")
var sendEmail = flag.Bool("se", false, "whether or not send email when error")

func main() {
	flag.Parse()
	defer func() {
		if *sendEmail {
			SendEmail()
		}
	}()
	logFile, err := os.Create("log.txt")
	openAlex.PanicError(err)
	log.SetOutput(logFile)
	log.Printf("totalpath : %s\n", *rootPath)
	log.Printf("startFile : %s\n", *startFile)
	openAlex.TotalPath = *rootPath
	openAlex.StartDir = *startDir
	openAlex.LogDetail = *logDetail
	log.Println("welcome to importer")
	if *importType == "authors" {
		openAlex.ImportScholars()
	} else if *importType == "papers" {
		log.Println("start to import papers")
		openAlex.GetPaperImporterContext(*rootPath, *startDir, *startFile, *logDetail).Import()
	} else {
		log.Println("start to import authors")
		openAlex.PanicError(errors.New("type argument is not authors or paper neither"))
	}
}

func SendEmail() {
	log.Printf("send email to 20373389@buaa.edu.cn\n")
	e := email.NewEmail()
	e.From = fmt.Sprintf("您的程序 <1838940019@qq.com>")
	e.To = []string{"20373389@buaa.edu.cn"}
	//设置文件发送的内容
	content := fmt.Sprintf(`您的程序又又又崩了， 请登陆华为云查看`)
	e.HTML = []byte(content)
	e.Subject = "您的程序又又又崩了"
	//设置服务器相关的配置
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
