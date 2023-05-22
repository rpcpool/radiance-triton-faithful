package parse_legacy_transaction_status_meta

import (
	"fmt"

	"github.com/novifinancial/serde-reflection/serde-generate/runtime/golang/bincode"
	"github.com/novifinancial/serde-reflection/serde-generate/runtime/golang/serde"
)

type CompiledInstruction struct {
	ProgramIdIndex uint8
	Accounts       struct {
		Field0 struct{ Field0 uint8 }
		Field1 uint8
		Field2 uint8
		Field3 uint8
	}
	Data struct {
		Field0 struct{ Field0 uint8 }
		Field1 uint8
		Field2 uint8
		Field3 uint8
	}
}

func (obj *CompiledInstruction) Serialize(serializer serde.Serializer) error {
	if err := serializer.IncreaseContainerDepth(); err != nil {
		return err
	}
	if err := serializer.SerializeU8(obj.ProgramIdIndex); err != nil {
		return err
	}
	if err := serialize_tuple4_tuple1_u8_u8_u8_u8(obj.Accounts, serializer); err != nil {
		return err
	}
	if err := serialize_tuple4_tuple1_u8_u8_u8_u8(obj.Data, serializer); err != nil {
		return err
	}
	serializer.DecreaseContainerDepth()
	return nil
}

func (obj *CompiledInstruction) BincodeSerialize() ([]byte, error) {
	if obj == nil {
		return nil, fmt.Errorf("Cannot serialize null object")
	}
	serializer := bincode.NewSerializer()
	if err := obj.Serialize(serializer); err != nil {
		return nil, err
	}
	return serializer.GetBytes(), nil
}

func DeserializeCompiledInstruction(deserializer serde.Deserializer) (CompiledInstruction, error) {
	var obj CompiledInstruction
	if err := deserializer.IncreaseContainerDepth(); err != nil {
		return obj, err
	}
	if val, err := deserializer.DeserializeU8(); err == nil {
		obj.ProgramIdIndex = val
	} else {
		return obj, err
	}
	if val, err := deserialize_tuple4_tuple1_u8_u8_u8_u8(deserializer); err == nil {
		obj.Accounts = val
	} else {
		return obj, err
	}
	if val, err := deserialize_tuple4_tuple1_u8_u8_u8_u8(deserializer); err == nil {
		obj.Data = val
	} else {
		return obj, err
	}
	deserializer.DecreaseContainerDepth()
	return obj, nil
}

func BincodeDeserializeCompiledInstruction(input []byte) (CompiledInstruction, error) {
	if input == nil {
		var obj CompiledInstruction
		return obj, fmt.Errorf("Cannot deserialize null array")
	}
	deserializer := bincode.NewDeserializer(input)
	obj, err := DeserializeCompiledInstruction(deserializer)
	if err == nil && deserializer.GetBufferOffset() < uint64(len(input)) {
		return obj, fmt.Errorf("Some input bytes were not read")
	}
	return obj, err
}

type InnerInstructions struct {
	Index        uint8
	Instructions []CompiledInstruction
}

func (obj *InnerInstructions) Serialize(serializer serde.Serializer) error {
	if err := serializer.IncreaseContainerDepth(); err != nil {
		return err
	}
	if err := serializer.SerializeU8(obj.Index); err != nil {
		return err
	}
	if err := serialize_vector_CompiledInstruction(obj.Instructions, serializer); err != nil {
		return err
	}
	serializer.DecreaseContainerDepth()
	return nil
}

func (obj *InnerInstructions) BincodeSerialize() ([]byte, error) {
	if obj == nil {
		return nil, fmt.Errorf("Cannot serialize null object")
	}
	serializer := bincode.NewSerializer()
	if err := obj.Serialize(serializer); err != nil {
		return nil, err
	}
	return serializer.GetBytes(), nil
}

func DeserializeInnerInstructions(deserializer serde.Deserializer) (InnerInstructions, error) {
	var obj InnerInstructions
	if err := deserializer.IncreaseContainerDepth(); err != nil {
		return obj, err
	}
	if val, err := deserializer.DeserializeU8(); err == nil {
		obj.Index = val
	} else {
		return obj, err
	}
	if val, err := deserialize_vector_CompiledInstruction(deserializer); err == nil {
		obj.Instructions = val
	} else {
		return obj, err
	}
	deserializer.DecreaseContainerDepth()
	return obj, nil
}

func BincodeDeserializeInnerInstructions(input []byte) (InnerInstructions, error) {
	if input == nil {
		var obj InnerInstructions
		return obj, fmt.Errorf("Cannot deserialize null array")
	}
	deserializer := bincode.NewDeserializer(input)
	obj, err := DeserializeInnerInstructions(deserializer)
	if err == nil && deserializer.GetBufferOffset() < uint64(len(input)) {
		return obj, fmt.Errorf("Some input bytes were not read")
	}
	return obj, err
}

type Result interface {
	isResult()
	Serialize(serializer serde.Serializer) error
	BincodeSerialize() ([]byte, error)
}

func DeserializeResult(deserializer serde.Deserializer) (Result, error) {
	index, err := deserializer.DeserializeVariantIndex()
	if err != nil {
		return nil, err
	}

	switch index {
	case 0:
		if val, err := load_Result__Ok(deserializer); err == nil {
			return &val, nil
		} else {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("Unknown variant index for Result: %d", index)
	}
}

func BincodeDeserializeResult(input []byte) (Result, error) {
	if input == nil {
		var obj Result
		return obj, fmt.Errorf("Cannot deserialize null array")
	}
	deserializer := bincode.NewDeserializer(input)
	obj, err := DeserializeResult(deserializer)
	if err == nil && deserializer.GetBufferOffset() < uint64(len(input)) {
		return obj, fmt.Errorf("Some input bytes were not read")
	}
	return obj, err
}

type Result__Ok struct{}

func (*Result__Ok) isResult() {}

func (obj *Result__Ok) Serialize(serializer serde.Serializer) error {
	if err := serializer.IncreaseContainerDepth(); err != nil {
		return err
	}
	serializer.SerializeVariantIndex(0)
	if err := serializer.SerializeUnit(((struct{})(*obj))); err != nil {
		return err
	}
	serializer.DecreaseContainerDepth()
	return nil
}

func (obj *Result__Ok) BincodeSerialize() ([]byte, error) {
	if obj == nil {
		return nil, fmt.Errorf("Cannot serialize null object")
	}
	serializer := bincode.NewSerializer()
	if err := obj.Serialize(serializer); err != nil {
		return nil, err
	}
	return serializer.GetBytes(), nil
}

func load_Result__Ok(deserializer serde.Deserializer) (Result__Ok, error) {
	var obj struct{}
	if err := deserializer.IncreaseContainerDepth(); err != nil {
		return (Result__Ok)(obj), err
	}
	if val, err := deserializer.DeserializeUnit(); err == nil {
		obj = val
	} else {
		return ((Result__Ok)(obj)), err
	}
	deserializer.DecreaseContainerDepth()
	return (Result__Ok)(obj), nil
}

type TransactionStatusMeta struct {
	Status            Result
	Fee               uint64
	PreBalances       []uint64
	PostBalances      []uint64
	InnerInstructions *[]InnerInstructions
}

func (obj *TransactionStatusMeta) Serialize(serializer serde.Serializer) error {
	if err := serializer.IncreaseContainerDepth(); err != nil {
		return err
	}
	if err := obj.Status.Serialize(serializer); err != nil {
		return err
	}
	if err := serializer.SerializeU64(obj.Fee); err != nil {
		return err
	}
	if err := serialize_vector_u64(obj.PreBalances, serializer); err != nil {
		return err
	}
	if err := serialize_vector_u64(obj.PostBalances, serializer); err != nil {
		return err
	}
	if err := serialize_option_vector_InnerInstructions(obj.InnerInstructions, serializer); err != nil {
		return err
	}
	serializer.DecreaseContainerDepth()
	return nil
}

func (obj *TransactionStatusMeta) BincodeSerialize() ([]byte, error) {
	if obj == nil {
		return nil, fmt.Errorf("Cannot serialize null object")
	}
	serializer := bincode.NewSerializer()
	if err := obj.Serialize(serializer); err != nil {
		return nil, err
	}
	return serializer.GetBytes(), nil
}

func DeserializeTransactionStatusMeta(deserializer serde.Deserializer) (TransactionStatusMeta, error) {
	var obj TransactionStatusMeta
	if err := deserializer.IncreaseContainerDepth(); err != nil {
		return obj, err
	}
	if val, err := DeserializeResult(deserializer); err == nil {
		obj.Status = val
	} else {
		return obj, err
	}
	if val, err := deserializer.DeserializeU64(); err == nil {
		obj.Fee = val
	} else {
		return obj, err
	}
	if val, err := deserialize_vector_u64(deserializer); err == nil {
		obj.PreBalances = val
	} else {
		return obj, err
	}
	if val, err := deserialize_vector_u64(deserializer); err == nil {
		obj.PostBalances = val
	} else {
		return obj, err
	}
	if val, err := deserialize_option_vector_InnerInstructions(deserializer); err == nil {
		obj.InnerInstructions = val
	} else {
		return obj, err
	}
	deserializer.DecreaseContainerDepth()
	return obj, nil
}

func BincodeDeserializeTransactionStatusMeta(input []byte) (TransactionStatusMeta, error) {
	if input == nil {
		var obj TransactionStatusMeta
		return obj, fmt.Errorf("Cannot deserialize null array")
	}
	deserializer := bincode.NewDeserializer(input)
	obj, err := DeserializeTransactionStatusMeta(deserializer)
	if err == nil && deserializer.GetBufferOffset() < uint64(len(input)) {
		return obj, fmt.Errorf("Some input bytes were not read")
	}
	return obj, err
}

func serialize_option_vector_InnerInstructions(value *[]InnerInstructions, serializer serde.Serializer) error {
	if value != nil {
		if err := serializer.SerializeOptionTag(true); err != nil {
			return err
		}
		if err := serialize_vector_InnerInstructions((*value), serializer); err != nil {
			return err
		}
	} else {
		if err := serializer.SerializeOptionTag(false); err != nil {
			return err
		}
	}
	return nil
}

func deserialize_option_vector_InnerInstructions(deserializer serde.Deserializer) (*[]InnerInstructions, error) {
	tag, err := deserializer.DeserializeOptionTag()
	if err != nil {
		return nil, err
	}
	if tag {
		value := new([]InnerInstructions)
		if val, err := deserialize_vector_InnerInstructions(deserializer); err == nil {
			*value = val
		} else {
			return nil, err
		}
		return value, nil
	} else {
		return nil, nil
	}
}

func serialize_tuple1_u8(value struct{ Field0 uint8 }, serializer serde.Serializer) error {
	if err := serializer.SerializeU8(value.Field0); err != nil {
		return err
	}
	return nil
}

func deserialize_tuple1_u8(deserializer serde.Deserializer) (struct{ Field0 uint8 }, error) {
	var obj struct{ Field0 uint8 }
	if val, err := deserializer.DeserializeU8(); err == nil {
		obj.Field0 = val
	} else {
		return obj, err
	}
	return obj, nil
}

func serialize_tuple4_tuple1_u8_u8_u8_u8(value struct {
	Field0 struct{ Field0 uint8 }
	Field1 uint8
	Field2 uint8
	Field3 uint8
}, serializer serde.Serializer,
) error {
	if err := serialize_tuple1_u8(value.Field0, serializer); err != nil {
		return err
	}
	if err := serializer.SerializeU8(value.Field1); err != nil {
		return err
	}
	if err := serializer.SerializeU8(value.Field2); err != nil {
		return err
	}
	if err := serializer.SerializeU8(value.Field3); err != nil {
		return err
	}
	return nil
}

func deserialize_tuple4_tuple1_u8_u8_u8_u8(deserializer serde.Deserializer) (struct {
	Field0 struct{ Field0 uint8 }
	Field1 uint8
	Field2 uint8
	Field3 uint8
}, error,
) {
	var obj struct {
		Field0 struct{ Field0 uint8 }
		Field1 uint8
		Field2 uint8
		Field3 uint8
	}
	if val, err := deserialize_tuple1_u8(deserializer); err == nil {
		obj.Field0 = val
	} else {
		return obj, err
	}
	if val, err := deserializer.DeserializeU8(); err == nil {
		obj.Field1 = val
	} else {
		return obj, err
	}
	if val, err := deserializer.DeserializeU8(); err == nil {
		obj.Field2 = val
	} else {
		return obj, err
	}
	if val, err := deserializer.DeserializeU8(); err == nil {
		obj.Field3 = val
	} else {
		return obj, err
	}
	return obj, nil
}

func serialize_vector_CompiledInstruction(value []CompiledInstruction, serializer serde.Serializer) error {
	if err := serializer.SerializeLen(uint64(len(value))); err != nil {
		return err
	}
	for _, item := range value {
		if err := item.Serialize(serializer); err != nil {
			return err
		}
	}
	return nil
}

func deserialize_vector_CompiledInstruction(deserializer serde.Deserializer) ([]CompiledInstruction, error) {
	length, err := deserializer.DeserializeLen()
	if err != nil {
		return nil, err
	}
	obj := make([]CompiledInstruction, length)
	for i := range obj {
		if val, err := DeserializeCompiledInstruction(deserializer); err == nil {
			obj[i] = val
		} else {
			return nil, err
		}
	}
	return obj, nil
}

func serialize_vector_InnerInstructions(value []InnerInstructions, serializer serde.Serializer) error {
	if err := serializer.SerializeLen(uint64(len(value))); err != nil {
		return err
	}
	for _, item := range value {
		if err := item.Serialize(serializer); err != nil {
			return err
		}
	}
	return nil
}

func deserialize_vector_InnerInstructions(deserializer serde.Deserializer) ([]InnerInstructions, error) {
	length, err := deserializer.DeserializeLen()
	if err != nil {
		return nil, err
	}
	obj := make([]InnerInstructions, length)
	for i := range obj {
		if val, err := DeserializeInnerInstructions(deserializer); err == nil {
			obj[i] = val
		} else {
			return nil, err
		}
	}
	return obj, nil
}

func serialize_vector_u64(value []uint64, serializer serde.Serializer) error {
	if err := serializer.SerializeLen(uint64(len(value))); err != nil {
		return err
	}
	for _, item := range value {
		if err := serializer.SerializeU64(item); err != nil {
			return err
		}
	}
	return nil
}

func deserialize_vector_u64(deserializer serde.Deserializer) ([]uint64, error) {
	length, err := deserializer.DeserializeLen()
	if err != nil {
		return nil, err
	}
	obj := make([]uint64, length)
	for i := range obj {
		if val, err := deserializer.DeserializeU64(); err == nil {
			obj[i] = val
		} else {
			return nil, err
		}
	}
	return obj, nil
}
