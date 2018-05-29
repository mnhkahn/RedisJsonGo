package main

import (
	"encoding/json"
	"fmt"
	"unsafe"

	"github.com/wenerme/go-rm/rm"
)

var ModuleType rm.ModuleType

func main() {
	// In case someone try to run this
	rm.Run()
}

// 必须放在init里面
func init() {
	rm.Mod = CreateMyMod()
}

func CreateMyMod() *rm.Module {
	mod := rm.NewMod()
	mod.Name = "json"
	mod.Version = 1
	mod.Commands = []rm.Command{CreateCommand_ECHO(),
		CreateCommand_JSONSET(),
		CreateCommand_JSONGET(),
	}
	mod.DataTypes = []rm.DataType{CreateDataType()}
	return mod
}

func CreateCommand_ECHO() rm.Command {
	return rm.Command{
		Usage:    "print message",
		Desc:     `like echo.`,
		Name:     "print",
		Flags:    "",
		FirstKey: 1, LastKey: 1, KeyStep: 1,
		Action: func(cmd rm.CmdContext) int {
			ctx, args := cmd.Ctx, cmd.Args
			if len(args) != 2 {
				return ctx.WrongArity()
			}
			ctx.ReplyWithString(args[1])
			return rm.OK
		},
	}
}

func CreateCommand_JSONSET() rm.Command {
	return rm.Command{
		Usage:    `json.set a {"foo":"bar","baz":42}`,
		Desc:     `store a json object.`,
		Name:     "json.set",
		Flags:    "",
		FirstKey: 1, LastKey: 1, KeyStep: 1,
		Action: func(cmd rm.CmdContext) int {
			ctx, args := cmd.Ctx, cmd.Args
			if len(args) != 3 {
				return ctx.WrongArity()
			}

			ctx.AutoMemory()
			key, ok := openHashKey(ctx, args[1])
			if !ok {
				return rm.ERR
			}

			raw := args[2].String()
			rm.LogDebug("raw: %s", raw)

			val := new(JsonData)
			val.Name = args[1]
			val.data = make(map[string]interface{}, 1)
			err := json.Unmarshal([]byte(raw), &val.data)
			if err != nil {
				ctx.ReplyWithError(fmt.Sprintf("ERR %v", err))
				return rm.ERR
			}
			rm.LogDebug("json %v", val)

			if key.ModuleTypeSetValue(ModuleType, unsafe.Pointer(val)) == rm.ERR {
				ctx.ReplyWithError("ERR Failed to set module type value")
				return rm.ERR
			}

			ctx.ReplyWithString(args[2])
			return rm.OK
		},
	}
}

func CreateCommand_JSONGET() rm.Command {
	return rm.Command{
		Usage:    `json.get a foo`,
		Desc:     `get a json object.`,
		Name:     "json.get",
		Flags:    "",
		FirstKey: 1, LastKey: 1, KeyStep: 1,
		Action: func(cmd rm.CmdContext) int {
			ctx, args := cmd.Ctx, cmd.Args
			if len(args) < 2 {
				return ctx.WrongArity()
			}

			key, ok := openHashKey(ctx, args[1])
			if !ok {
				return rm.ERR
			}

			val := (*JsonData)(key.ModuleTypeGetValue())
			rm.LogDebug("raw: %v", val)

			if val == nil || val.data == nil {
				ctx.ReplyWithSimpleString("data is nil.")
				return rm.OK
			}

			resLen := len(args[2:])

			var resMap map[string]interface{}
			if resLen == 0 {
				resMap = make(map[string]interface{}, len(val.data))
				for k, v := range val.data {
					resMap[k] = v
				}
			} else {
				resMap = make(map[string]interface{}, resLen)
				for _, arg := range args[2:] {
					a := arg.String()
					rm.LogDebug(a)

					resMap[a] = val.data[a]
				}
			}

			rm.LogDebug("AA %v", resMap)

			res, err := json.Marshal(resMap)
			if err != nil {
				ctx.ReplyWithError(err.Error())
				return rm.ERR
			}

			ctx.ReplyWithSimpleString(string(res))
			return rm.OK
		},
	}
}

// open the key and make sure it is indeed a Hash and not empty
func openHashKey(ctx rm.Ctx, k rm.String) (rm.Key, bool) {
	key := ctx.OpenKey(k, rm.READ|rm.WRITE)
	rm.LogDebug("keytype: %d", key.KeyType())

	if !key.IsEmpty() && key.ModuleTypeGetType() != ModuleType {
		ctx.ReplyWithError(rm.ERRORMSG_WRONGTYPE)
		return rm.Key(0), false
	}
	return key, true
}
