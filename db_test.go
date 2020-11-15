package paxi

import (
	"testing"
)

func TestCommand_Equal(t *testing.T) {
	v1 := GenerateRandVal(8)
	cmd1 := Command{
		Key:       42,
		Value:     v1,
		ClientID:  "1.1",
		CommandID: 10,
	}

	cmd2 := &Command{
		Key:       42,
		Value:     v1,
		ClientID:  "1.1",
		CommandID: 10,
	}

	eq := cmd1.Equal(cmd2)
	if !eq {
		t.Errorf("Expected command equality, got eq=%t", eq)
	}

	v2 := GenerateRandVal(8)
	cmd3 := &Command{
		Key:       42,
		Value:     v2,
		ClientID:  "1.1",
		CommandID: 10,
	}

	eq = cmd1.Equal(cmd3)
	if eq {
		t.Errorf("Expected command inequality, got eq=%t", eq)
	}
}