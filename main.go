package main

import (
	"fmt"
	"github.com/iesreza/agdb/agdb"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"time"
)

func main() {
	agg := agdb.NewAggregator(agdb.Minute, 3000*time.Second)
	var fileError error
	start := time.Now()

	if fileExists("./save.json") {
		b, err := ioutil.ReadFile("./save.json") // b has type []byte
		if err != nil {
			log.Fatal(err)
		}
		agg.Load(b)
	}

	file, exist := os.Create("./save.json")
	if exist != nil {
		file, fileError = os.Open("./save.json")
		if fileError != nil {
			panic(fileError)
		}
	}

	go func() {
		for {
			fmt.Printf("%+v\r\n", agg.Get([]string{"test"}, start, time.Now()))
			fmt.Printf("%+v\r\n", agg.Get([]string{}, start, time.Now()))
			time.Sleep(5 * time.Second)
			b, err := agg.Pack()
			if err == nil {
				file.Write(b)
			}
		}
	}()

	s := 0
	for {
		n := rand.Intn(7)
		agg.Increment("test", n)
		agg.Increment("test2", n+1)
		agg.Increment("test3", n+4)
		s += n
		time.Sleep(time.Duration(1) * time.Second)
		fmt.Printf("Rand:%d Sum:%d \r\n", n, s)
	}

}
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
