package utils

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/hamba/avro"
)

func ParseJsonFileToSchemaAvroByte(jsonFilePath, valueSchema string) ([]byte, error) {
	schema, err := avro.Parse(valueSchema)

	if err != nil {
		return nil, err
	}

	jsonFile, err := os.Open(jsonFilePath)

	if err != nil {
		return nil, err
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	generic := map[string]interface{}{}

	if err := json.Unmarshal(byteValue, &generic); err != nil {
		panic(err)
	}

	fmt.Println(generic)

	data, err := avro.Marshal(schema, generic)

	if err != nil {
		return nil, err
	}

	return data, nil
}

func GetIdFromAvro(bytes []byte) (int32, error) {

	if bytes == nil {
		return 0, nil
	}

	if bytes[0] != 0 {
		return 0, errors.New("magic number not found")
	}

	id := int32(binary.BigEndian.Uint32(bytes[1:]))

	return id, nil
}

func SetIdToAvroJson(jsonAvro []byte, id uint32) []byte {
	zero := make([]byte, 1)

	zero[0] = 0

	magicNumber := make([]byte, 5)

	binary.BigEndian.PutUint32(magicNumber, 12345)

	jsonAvro = append(magicNumber, jsonAvro...)
	jsonAvro = append(zero, jsonAvro...)

	return jsonAvro
}
