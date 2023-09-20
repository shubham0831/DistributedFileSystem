package main

import "strings"

func Map(lineNumber int, line string) map[string]int {
	tokenized := strings.Split(line, " ")

	countMap := make(map[string]int)

	for _, word := range tokenized {
		if word == " " {
			continue
		}

		if _, present := countMap[word]; !present {
			countMap[word] = 0
		}

		countMap[word]++
	}

	return countMap
}

// Reduce // word is the key, and countList is the count of the word
// count list will most likely just be an array of 1's
func Reduce(word string, countList []int) (string, int64) {
	var count int64

	for _, countVal := range countList {
		count += int64(countVal)
	}

	return word, count
}
