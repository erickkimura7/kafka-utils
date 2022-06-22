package kafka_utils

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/hamba/avro"
)

func teste() {
	type SimpleRecord struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}

	schema, err := avro.Parse(`{
		"type": "record",
		"name": "simple",
		"namespace": "org.hamba.avro",
		"fields" : [
			{"name": "a", "type": "string"},
			{"name": "b", "type": "string"},
			{"name": "c", "type": "string", "default": ""}
		]
	}`)

	schema1, err := avro.Parse(`{
		"type": "record",
		"name": "simple",
		"namespace": "org.hamba.avro",
		"fields" : [
			{"name": "a", "type": "string"},
			{"name": "b", "type": "string"},
			{"name": "c", "type": "string", "default": ""},
			{"name": "d", "type": "string", "default": ""}
		]
	}`)
	if err != nil {
		log.Fatal(err)
	}

	// in := SimpleRecord{A: 27, B: "foo"}

	jsonFile, err := os.Open("teste.json")

	if err != nil {
		log.Fatal(err)
	}

	defer jsonFile.Close()

	// byteValue, _ := ioutil.ReadAll(jsonFile)

	dat := map[string]interface{}{
		"a": "123",
		"b": "123",
	}

	// if err := json.Unmarshal(byteValue, &dat); err != nil {
	// 	panic(err)
	// }
	// fmt.Println(dat)

	data, err := avro.Marshal(schema, dat)
	if err != nil {
		log.Fatal(err)
	}

	data = SetIdToAvroJson(data, 12345)

	fmt.Println(string(data))
	// Outputs: [54 6 102 111 111]

	GetIdFromAvro(data)

	var out interface{}
	err = avro.Unmarshal(schema1, data[5:], &out)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(out)
	// Outputs: {27 foo}

	jsonStr, err := json.Marshal(out)

	if err != nil {
		log.Fatal(err)
	}

	log.Println(string(jsonStr))
}
