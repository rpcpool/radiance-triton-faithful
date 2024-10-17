package createcar

import (
	"fmt"
	"strconv"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	solanaerrors "github.com/rpcpool/yellowstone-faithful/solana-errors"
	"github.com/rpcpool/yellowstone-faithful/third_party/solana_proto/confirmed_block"
)

func fromRpcTxToProtobufTxMeta(rpcTx *rpc.TransactionWithMeta) (*confirmed_block.TransactionStatusMeta, error) {
	if rpcTx.Meta == nil {
		return nil, fmt.Errorf("rpcTx.Meta is nil")
	}
	in := rpcTx.Meta

	var out confirmed_block.TransactionStatusMeta

	{
		if in.Err != nil {
			out.Err = new(confirmed_block.TransactionError)
			if asMap, ok := in.Err.(map[string]interface{}); ok {
				encodedError, err := solanaerrors.FromJSONToProtobuf(asMap)
				if err != nil {
					return nil, fmt.Errorf("failed to convert error: %w", err)
				}
				out.Err.Err = encodedError
			}
		}

		out.Fee = in.Fee
		out.PreBalances = in.PreBalances
		out.PostBalances = in.PostBalances
		out.InnerInstructions = innerInstructionsToProtobuf(in.InnerInstructions)
		// out.InnerInstructionsNone = in.InnerInstructionsNone
		out.LogMessages = in.LogMessages
		// out.LogMessagesNone = in.LogMessagesNone
		out.PreTokenBalances = tokenBalancesToProtobuf(in.PreTokenBalances)
		out.PostTokenBalances = tokenBalancesToProtobuf(in.PostTokenBalances)
		out.Rewards = rewardsToProtobuf(in.Rewards)
		out.LoadedWritableAddresses = in.LoadedAddresses.Writable.ToBytes()
		out.LoadedReadonlyAddresses = in.LoadedAddresses.ReadOnly.ToBytes()
		out.ReturnData = returnDataToProtobuf(in.ReturnData)
		// out.ReturnDataNone = in.ReturnDataNone
		out.ComputeUnitsConsumed = in.ComputeUnitsConsumed
	}

	return &out, nil
}

func returnDataToProtobuf(data rpc.ReturnData) *confirmed_block.ReturnData {
	return &confirmed_block.ReturnData{
		ProgramId: data.ProgramId.Bytes(),
		Data:      data.Data.Content,
	}
}

func rewardsToProtobuf(rewards []rpc.BlockReward) []*confirmed_block.Reward {
	out := make([]*confirmed_block.Reward, len(rewards))
	for i, reward := range rewards {
		out[i] = &confirmed_block.Reward{
			Pubkey:      reward.Pubkey.String(),
			Lamports:    reward.Lamports,
			PostBalance: reward.PostBalance,
			//
			RewardType: func() confirmed_block.RewardType {
				switch reward.RewardType {
				case rpc.RewardTypeFee:
					return confirmed_block.RewardType_Fee
				case rpc.RewardTypeRent:
					return confirmed_block.RewardType_Rent
				case rpc.RewardTypeVoting:
					return confirmed_block.RewardType_Voting
				case rpc.RewardTypeStaking:
					return confirmed_block.RewardType_Staking
				default:
					panic(fmt.Errorf("unknown reward type: %v", reward.RewardType))
				}
			}(),
			Commission: func() string {
				if reward.Commission == nil {
					return ""
				}
				// TODO: is this correct?
				return strconv.Itoa(int(*reward.Commission))
			}(),
		}
	}
	return out
}

func tokenBalancesToProtobuf(balances []rpc.TokenBalance) []*confirmed_block.TokenBalance {
	out := make([]*confirmed_block.TokenBalance, len(balances))
	for i, balance := range balances {
		out[i] = &confirmed_block.TokenBalance{
			AccountIndex: uint32(balance.AccountIndex),
			Mint:         balance.Mint.String(),
			UiTokenAmount: &confirmed_block.UiTokenAmount{
				Amount:   balance.UiTokenAmount.Amount,
				Decimals: uint32(balance.UiTokenAmount.Decimals),
				UiAmount: func() float64 {
					if balance.UiTokenAmount.UiAmount == nil {
						return 0
					}
					return *balance.UiTokenAmount.UiAmount
				}(),
				UiAmountString: balance.UiTokenAmount.UiAmountString,
			},
		}
	}
	return out
}

func innerInstructionsToProtobuf(instructions []rpc.InnerInstruction) []*confirmed_block.InnerInstructions {
	out := make([]*confirmed_block.InnerInstructions, len(instructions))
	for i, instruction := range instructions {
		out[i] = &confirmed_block.InnerInstructions{
			Index:        uint32(instruction.Index),
			Instructions: solanaCompiledInstructionsToProtobuf(instruction.Instructions),
		}
	}
	return out
}

func solanaCompiledInstructionsToProtobuf(instructions []solana.CompiledInstruction) []*confirmed_block.InnerInstruction {
	out := make([]*confirmed_block.InnerInstruction, len(instructions))
	for i, instr := range instructions {
		out[i] = solanaCompiledInstructionToProtobuf(instr)
	}
	return out
}

func solanaCompiledInstructionToProtobuf(instruction solana.CompiledInstruction) *confirmed_block.InnerInstruction {
	return &confirmed_block.InnerInstruction{
		ProgramIdIndex: uint32(instruction.ProgramIDIndex),
		Accounts: func() []byte {
			out := make([]byte, len(instruction.Accounts))
			for i, acc := range instruction.Accounts {
				out[i] = byte(acc)
			}
			return out
		}(),
		Data: instruction.Data,
		// StackHeight: uint32(instruction.StackHeight), // TODO.
	}
}
