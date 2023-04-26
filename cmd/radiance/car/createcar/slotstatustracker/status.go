package slotstatustracker

type SlotStatus uint8

const (
	SlotStatusNull      SlotStatus = iota // 0 = null
	SlotStatusPackaging                   // 1 = packaging the slot
	SlotStatusPackaged                    // 2 = successfully packaged the slot and wrote it to disk
	SlotStatusIncluded                    // 3 = included the slot in the DAG
)

func isValidSlotStatus(status SlotStatus) bool {
	return status.IsAny(
		SlotStatusNull,
		SlotStatusPackaging,
		SlotStatusPackaged,
		SlotStatusIncluded,
	)
}

func (s SlotStatus) Is(status SlotStatus) bool {
	return s == status
}

func (s SlotStatus) IsAny(statuses ...SlotStatus) bool {
	for _, status := range statuses {
		if s == status {
			return true
		}
	}
	return false
}
