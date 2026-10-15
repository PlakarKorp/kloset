package header

import (
	"encoding/json"

	"github.com/PlakarKorp/kloset/snapshot/vfs"
)

type classCount struct {
	Key   string `json:"key"`
	Value uint64 `json:"value"`
}

// classificationJSON mirrors the JSON form of vfs.Classification, which API
// consumers parse for files and snapshots alike: class names in "classes",
// probabilities and counts in parallel lists.
type classificationJSON struct {
	Analyzer string       `json:"analyzer"`
	Classes  []string     `json:"classes"`
	Scores   []vfs.Score  `json:"scores"`
	Counts   []classCount `json:"counts"`
}

func (c Classification) MarshalJSON() ([]byte, error) {
	out := classificationJSON{
		Analyzer: c.Analyzer,
		Classes:  make([]string, 0, len(c.Classes)),
		Scores:   make([]vfs.Score, 0, len(c.Classes)),
		Counts:   make([]classCount, 0, len(c.Classes)),
	}
	for _, class := range c.Classes {
		out.Classes = append(out.Classes, class.Name)
		out.Scores = append(out.Scores, vfs.Score{Key: class.Name, Value: class.Probability})
		out.Counts = append(out.Counts, classCount{Key: class.Name, Value: class.Count})
	}
	return json.Marshal(out)
}

func (c *Classification) UnmarshalJSON(data []byte) error {
	var in classificationJSON
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}

	scores := make(map[string]float64, len(in.Scores))
	for _, s := range in.Scores {
		scores[s.Key] = s.Value
	}
	counts := make(map[string]uint64, len(in.Counts))
	for _, n := range in.Counts {
		counts[n.Key] = n.Value
	}

	c.Analyzer = in.Analyzer
	c.Classes = make([]Class, 0, len(in.Classes))
	for _, name := range in.Classes {
		c.Classes = append(c.Classes, Class{Name: name, Probability: scores[name], Count: counts[name]})
	}
	return nil
}
