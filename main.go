package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

type ElasticsearchResponse struct {
	Id string `json:"_id"`
}

type Action string

const (
	CreateDocument Action = "CreateDocument"
)

type Operation struct {
	Action Action
	DocId  string
	Target string
}

var Operations []Operation

func httpCreateDocument(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)
	target := vars["target"]

	resp, err := http.Post(
		fmt.Sprintf("http://localhost:9200/%s/_doc/", target),
		"application/json",
		r.Body,
	)

	if err != nil {
		log.Fatal(err)
	}

	var response ElasticsearchResponse

	json.NewDecoder(resp.Body).Decode(&response)

	Operations = append(Operations, Operation{
		Action: CreateDocument,
		DocId:  response.Id,
		Target: target,
	})

	json.NewEncoder(w).Encode(response)
}

func httpOperations(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Operations)
}

func rollbackDeleteDocument(target string, docId string) map[string]interface{} {
	req, err := http.NewRequest(
		http.MethodDelete,
		fmt.Sprintf("http://localhost:9200/%s/_doc/%s", target, docId),
		nil,
	)

	if err != nil {
		log.Fatal(err)
	}

	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		log.Fatal(err)
	}

	var response map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&response)
	return response
}

func httpRollback(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	for index := len(Operations) - 1; index >= 0; index-- {
		operationToReverse := Operations[index]

		switch operationToReverse.Action {
		case CreateDocument:
			response := rollbackDeleteDocument(operationToReverse.Target, operationToReverse.DocId)
			json.NewEncoder(w).Encode(response)
		}
	}
}

func handleRequests() {
	router := mux.NewRouter()

	router.HandleFunc("/{target}/_doc", httpCreateDocument).Methods("POST")
	router.HandleFunc("/_operations", httpOperations).Methods("GET")
	router.HandleFunc("/_rollback", httpRollback).Methods("POST")

	log.Fatal(http.ListenAndServe(":10000", router))
}

func main() {
	handleRequests()
}
