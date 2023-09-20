package main

import "strings"

func Map(lineNumber int, line string) map[string]int {
	tokenizedArray := strings.Split(line, " ")
	url := tokenizedArray[len(tokenizedArray)-1]
	countMap := make(map[string]int)
	countMap[url] = 1
	return countMap
}

func Reduce(word string, countList []int) (string, int64) {
	count := int64(-1)

	for _, countVal := range countList {
		if count == -1 {
			count = int64(countVal)
			continue
		}

		if count > int64(countVal) {
			count = int64(countVal)
		}
	}

	return word, count
}
