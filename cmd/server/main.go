package main

import (
	"log"

	"github.com/samya-ak/godis/internal/server"
)

func main() {
	srv := server.NewHttpServer(":8080")
	log.Fatal(srv.ListenAndServe())
	log.Println("Listening ...")
}
