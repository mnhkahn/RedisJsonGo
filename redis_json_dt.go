package main

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/wenerme/go-rm/rm"
)

func CreateDataType() rm.DataType {
	return rm.DataType{
		Name:   "RedisJson",
		EncVer: 1,
		Desc:   "",
		// TODO Load and Save
		Free: func(ptr unsafe.Pointer) {
			rm.LogDebug("Free. not implement.")

			// 	val := (*rbData)(ptr)
			// 	err := val.Index.Close()
			// 	if err != nil {
			// 		rm.LogError("Free %v failed: %v", val.Name, err)
			// 	} else {
			// 		rm.LogDebug("Free %v", val.Name)
			// 	}
			// 	os.RemoveAll(val.Path)
		},
		RdbLoad: func(rdb rm.IO, encver int) unsafe.Pointer {
			rm.LogDebug("RdbLoad. not implement.")
			return nil
		},
		RdbSave: func(rdb rm.IO, value unsafe.Pointer) {
			rm.LogDebug("RdbSave. not implement.")
		},
		AofRewrite: func(aof rm.IO, key rm.String, value unsafe.Pointer) {
			rm.LogDebug("AofRewrite. not implement.")
		},
		Digest: func(digest rm.Digest, value unsafe.Pointer) {
			rm.LogDebug("Digest. not implement.")
		},
	}
}

type JsonData struct {
	Name rm.String
	data map[string]interface{}
}

func uintptrToBytes(u uintptr) []byte {
	size := unsafe.Sizeof(u)
	b := make([]byte, size)
	switch size {
	case 4:
		binary.LittleEndian.PutUint32(b, uint32(u))
	case 8:
		binary.LittleEndian.PutUint64(b, uint64(u))
	default:
		panic(fmt.Sprintf("unknown uintptr size: %v", size))
	}
	return b
}
