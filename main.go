package main

import (
	"net/http"
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"
)

type Result struct {
	URL      string
	Status   string
	Duration time.Duration
	Err      error
}

func worker(id int, jobs <- chan string, results chan<- Result, wg *sync.WaitGroup){
	defer wg.Done()

	client := &http.Client{
		Timeout: 5*time.Second,
	}

	for url := range jobs{
		start:= time.Now()
		resp,err := client.Get(url)
		duration := time.Since(start)

		res:= Result{
			URL: url,
			Duration: duration,
			Err: err,
		}
		if err!= nil{
			res.Status ="Failed"
		}else{
			res.Status = resp.Status
			resp.Body.Close()
		}
		results <-res
	}
}

func main() {
	startTime := time.Now()

	file, err := os.Open("url.txt")
	if err != nil {
		fmt.Printf("Error opening file : %v", err)
		os.Exit(1)
	}
	defer file.Close()
	var urls []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			urls = append(urls, line)
		}
	}
	numUrls := len(urls)
	if numUrls == 0 {
		fmt.Println("no url found in url.txt")
		return
	}

	jobs := make(chan string, numUrls)
	results := make(chan Result, numUrls)
	var wg sync.WaitGroup


	numWorkers := 3
	fmt.Printf("Starting %d concurrent workers to check %d URLs... \n\n", numWorkers,numUrls)

	for w:=1; w<= numWorkers; w++{
		wg.Add(1)
		go worker(w,jobs,results, &wg)
	}

	for _,url := range urls{
		jobs <- url
	}
	close(jobs)
	go func(){
		wg.Wait()
		close(results)
	}()

	for res:= range results{
		if res.Err != nil{
			fmt.Printf("[Error] %s -> Error : %v (Took %v) \n", res.URL,res.Err,res.Duration)
		}else{
			fmt.Printf("[OK] %s -> Status : %s (Took %v)\n",res.URL,res.Status,res.Duration)
		}
	}

	fmt.Printf("\nALL checks completed in %v\n", time.Since(startTime))

}
