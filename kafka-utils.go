package kafka_utils

import (
	"encoding/binary"
	"log"
)

func GetIdFromAvro(value []byte) int32 {
	id := int32(binary.BigEndian.Uint32(value[1:]))

	log.Println("id: ", id)

	return id
}

func SetIdToAvroJson(jsonAvro []byte, id uint32) []byte {
	zero := make([]byte, 1)

	zero[0] = 0

	magicNumber := make([]byte, 5)

	binary.BigEndian.PutUint32(magicNumber, 12345)

	jsonAvro = append(magicNumber, jsonAvro...)
	jsonAvro = append(zero, jsonAvro...)

	log.Println(string(jsonAvro))

	return jsonAvro
}
