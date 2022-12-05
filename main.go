package main

import (
	"flag"
	"io"
	"log"
	"os"
	"soft2_importer/openAlex"
)

var totalPath = flag.String("oa", "/home/diamond/soft2/data/openalex", "the config file")
var startDir = flag.String("sd", "", "the directory to start, if empty , start from newest directory")

func main() {
	logFile, err := os.Create("log.txt")
	openAlex.FatalError(err)
	log.SetOutput(logFile)
	flag.Parse()
	log.Printf("totalpath : %s\n", *totalPath)
	openAlex.TotalPath = *totalPath
	openAlex.StartDir = *startDir
	logOutput()
	log.Println("welcome to importer")
	openAlex.ImportScholars()
	//ImportPapers()
}
func logOutput() func() {
	logfile := `log.txt`
	// open file read/write | create if not exist | clear file at open if exists
	f, _ := os.OpenFile(logfile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)

	// save existing stdout | MultiWriter writes to saved stdout and file
	out := os.Stdout
	mw := io.MultiWriter(out, f)

	// get pipe reader and writer | writes to pipe writer come out pipe reader
	r, w, _ := os.Pipe()

	// replace stdout,stderr with pipe writer | all writes to stdout, stderr will go through pipe instead (fmt.print, log)
	os.Stdout = w
	os.Stderr = w

	// writes with log.Print should also write to mw
	log.SetOutput(mw)

	//create channel to control exit | will block until all copies are finished
	exit := make(chan bool)

	go func() {
		// copy all reads from pipe to multiwriter, which writes to stdout and file
		_, _ = io.Copy(mw, r)
		// when r or w is closed copy will finish and true will be sent to channel
		exit <- true
	}()

	// function to be deferred in main until program exits
	return func() {
		// close writer then block on exit channel | this will let mw finish writing before the program exits
		_ = w.Close()
		<-exit
		// close file after all writes have finished
		_ = f.Close()
	}

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
