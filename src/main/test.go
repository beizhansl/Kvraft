package main

import (
	"fmt"
	"strconv"
)

type KeyValue struct {
	Key   string
	Value string
}

func main() {
	kv := KeyValue{"1", "a"}
	intermediate := make([][]KeyValue, 3)
	intermediate[0] = append(intermediate[0], kv)
	fmt.Println(intermediate[0][0].Key)
	x := "x"
	x = x + strconv.Itoa(1)
	fmt.Println(x)
}
