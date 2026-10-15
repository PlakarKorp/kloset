package header

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestClassificationJSON(t *testing.T) {
	c := Classification{
		Analyzer: "secrets",
		Classes: []Class{
			{Name: "secret.private-key", Probability: 1, Count: 3},
			{Name: "secret.aws-access-key", Probability: 0.5, Count: 1},
		},
	}

	data, err := json.Marshal(c)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"analyzer": "secrets",
		"classes": ["secret.private-key", "secret.aws-access-key"],
		"scores": [{"key": "secret.private-key", "value": 1}, {"key": "secret.aws-access-key", "value": 0.5}],
		"counts": [{"key": "secret.private-key", "value": 3}, {"key": "secret.aws-access-key", "value": 1}]
	}`, string(data))

	var back Classification
	require.NoError(t, json.Unmarshal(data, &back))
	require.Equal(t, c, back)
}

func TestClassificationMsgpackUnchanged(t *testing.T) {
	c := Classification{
		Analyzer: "secrets",
		Classes:  []Class{{Name: "secret.private-key", Probability: 1, Count: 3}},
	}

	data, err := msgpack.Marshal(c)
	require.NoError(t, err)

	var fields map[string]any
	require.NoError(t, msgpack.Unmarshal(data, &fields))
	classes := fields["classes"].([]any)
	require.Equal(t, "secret.private-key", classes[0].(map[string]any)["name"])
}
