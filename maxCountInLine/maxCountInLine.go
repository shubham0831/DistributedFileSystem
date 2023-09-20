package main

import (
	"fmt"
	"strings"
)

func main() {
	line := "Matthew Matthew Matthew Gaurav Mat"
	mapOut := Map(0, line)

	for key, value := range mapOut {
		fmt.Printf("Map out for key = %s is %d\n", key, int(value))
	}

	fmt.Println()
	for key, value := range mapOut {
		sl := make([]int, 0)
		sl = append(sl, value)
		sl = append(sl, 1)
		sl = append(sl, 20)
		sl = append(sl, 1)
		sl = append(sl, 1)
		sl = append(sl, 1)

		redKey, redOut := Reduce(key, sl)
		fmt.Printf("Red out for key = %s is %d\n", redKey, int(redOut))
	}

}

// Map // lineNumber is the key, and line is the value
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

func Reduce(word string, countList []int) (string, int64) {
	count := int64(-1)

	for _, countVal := range countList {
		if count == -1 {
			count = int64(countVal)
			continue
		}

		if int64(countVal) > count {
			count = int64(countVal)
		}
	}

	return word, count
}
