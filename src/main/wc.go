package main

import (
"os"
"fmt"
"mapreduce"
"container/list"
"strings"
"strconv"
"unicode"
)

// Map 함수는 입력된 텍스트를 단어로 분할하고, 각 단어에 대해 key-value 쌍을 생성하여 반환합니다.
func Map(value string) *list.List {
// 결과를 저장할 리스트 생성
result := list.New()
// 텍스트를 공백 기준으로 분할하여 단어 리스트 생성
words := strings.FieldsFunc(value, func(c rune) bool {
// 특수 문자나 구두점을 제거하고 단어로 분할
return !unicode.IsLetter(c) && !unicode.IsNumber(c)
})
for _, word := range words {
word = strings.ToLower(word) // 단어를 소문자로 변환하여 중복 방지
// 각 단어의 빈도를 1로 설정하여 KeyValue 구조체로 리스트에 추가
result.PushBack(mapreduce.KeyValue{Key: word, Value: "1"})
}
return result
}

// Reduce 함수는 동일한 key에 대한 값 리스트를 입력받아, 그 값을 모두 합산한 결과를 반환합니다.
func Reduce(key string, values *list.List) string {
sum := 0
for e := values.Front(); e != nil; e = e.Next() {
// 값들을 숫자로 변환하여 합산
count, err := strconv.Atoi(e.Value.(string))
if err == nil {
sum += count
}
}
// 합산 결과를 문자열로 반환
return strconv.Itoa(sum)
}

// 프로그램 실행 방식에 따른 메인 함수
func main() {
if len(os.Args) != 4 {
fmt.Printf("%s: see usage comments in file\n", os.Args[0])
} else if os.Args[1] == "master" {
if os.Args[3] == "sequential" {
mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
} else {
mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])
// MR이 완료될 때까지 대기
<-mr.DoneChannel
}
} else {
mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
}
}
