package main

import (
	"fmt"
	"os"

	"github.com/Sirupsen/logrus"
)

// fatal prints the error's details if it is a libcontainer specific error type
// then exits the program with an exit status of 1.
func fatal(err error) {
	// make sure the error is written to the logger
	logrus.Error(err)
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func Uniq(list []string) []string {
	uniqset := make(map[string]bool, len(list))
	for _, x := range list {
		uniqset[x] = true
	}
	result := make([]string, 0, len(uniqset))
	for x := range uniqset {
		result = append(result, x)
	}
	return result
}

func MergeNumArray2NumRange(array []int) string {
	var i = 0
	var result = ""

	for j, value := range array {
		if j == len(array)-1 {
			if i == j {
				result += fmt.Sprintf("%d", array[j])
			} else {
				result += fmt.Sprintf("%d-%d", array[i], array[j])
			}
			break
		}

		if value != array[j+1]-1 {
			if j == i {
				result += fmt.Sprintf("%d,", array[i])
			} else {
				result += fmt.Sprintf("%d-%d,", array[i], array[j])
			}
			i = j + 1
		}
	}

	return result
}
