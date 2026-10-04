package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6380", Protocol: 2})

	keys, _ := rdb.Keys(ctx, "cache:*").Result()
	fmt.Printf("Keys found: %v\n", len(keys))
	if len(keys) > 0 {
		vec, err := rdb.HGet(ctx, keys[0], "vector").Result()
		if err != nil {
			fmt.Println("Error HGet:", err)
			return
		}
		
		fmt.Println("Vector bytes len:", len(vec))
		
		res, err := rdb.Do(ctx, "FT.SEARCH", "idx:cache", "*=>[KNN 1 @vector $query_vec AS score]", "PARAMS", "2", "query_vec", []byte(vec), "RETURN", "3", "response", "score", "text", "DIALECT", "2").Result()
		
		if err != nil {
			fmt.Println("FT.SEARCH Error:", err)
			return
		}
		
		fmt.Printf("Search result type: %T\n", res)
		if slice, ok := res.([]interface{}); ok {
			fmt.Println("Slice length:", len(slice))
			for i, v := range slice {
				fmt.Printf("Item %d (%T): %v\n", i, v, v)
			}
		}
	} else {
		fmt.Println("No keys to test with.")
	}
}
