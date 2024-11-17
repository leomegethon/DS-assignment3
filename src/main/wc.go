package main

import (
	"container/list"
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"mapreduce"
)

func Map(filename string) *list.List {
	infile, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil
	}
	defer infile.Close()

	// Initialize the list and wordCount map
	result := list.New()
	wordCount := make(map[string]int)
	scanner := bufio.NewScanner(infile)

	for scanner.Scan() {
		line := scanner.Text()
		words := strings.Fields(line) // Split by whitespace
		for _, word := range words {
			// Sanitize the word to avoid issues with special characters
			cleanWord := strings.Trim(word, `.,;:"'!?()[]`)
			if cleanWord != "" {
				wordCount[cleanWord]++
			}
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
		return nil
	}

	// Populate the list with KeyValue pairs
	for word, count := range wordCount {
		result.PushBack(mapreduce.KeyValue{Key: word, Value: fmt.Sprintf("%d", count)})
	}

	return result
}



func Reduce(key string, values *list.List) string {
	total := 0
	for e := values.Front(); e != nil; e = e.Next() {
		count, err := strconv.Atoi(e.Value.(string))
		if err != nil {
			fmt.Println("Error converting count:", err)
			continue
		}
		total += count
	}
	return fmt.Sprintf("%d", total)
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: wc.go master <file> <mode>")
		return
	}

	masterAddress := os.Args[1]
	fileName := os.Args[2]
	mode := os.Args[3]

	// MapReduce 작업 초기화
	nMap := 3
	nReduce := 3

	// MakeMapReduce 함수는 MapReduce 작업을 설정합니다.
	mr := mapreduce.MakeMapReduce(nMap, nReduce, fileName, masterAddress)

	// 실행 모드에 따라 다르게 실행
if mode == "sequential" {
	mapreduce.RunSingle(nMap, nReduce, fileName, Map, Reduce) // 독립 함수로 호출
} else if mode == "parallel" {
	mr.Run() // 병렬 모드에서 실행
} else {
	fmt.Println("Invalid mode. Use 'sequential' or 'parallel'.")
}

}
