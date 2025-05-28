package importer

import (
	"fmt"
	"strings"
	"sync"

	"github.com/PlakarKorp/kloset/kcontext"
)

// MultiImporter combines multiple Importers into one.
type MultiImporter struct {
	Importers []Importer
	name      string
}

// NewMultiImporter creates a new MultiImporter with the given Importers.
func NewMultiImporter(ctx *kcontext.KContext, name string, config map[string]string) (Importer, error) {
	importers := make([]Importer, 0)
	confs := make(map[string]map[string]string)

	//conf looks like this:
	// {
	// 	"name1.location": "location1",
	//  "name1.tag1": "value1",
	//  "name1.tag2": "value2",
	//  "name2.location": "location2",
	//  "name2.tag1": "value3",
	//  "name2.tag2": "value4",
	// }
	// confs, after this loop, should look like this:
	// {
	// 	"name1": {
	// 		"location": "location1",
	// 		"tag1": "value1",
	// 		"tag2": "value2",
	// 	},
	// 	"name2": {
	// 		"location": "location2",
	// 		"tag1": "value3",
	// 		"tag2": "value4",
	// 	},
	// }

	for key, value := range config {
		parts := strings.SplitN(key, ".", 2)
		if len(parts) != 2 {
			continue
		}
		impName := parts[0]
		tag := parts[1]

		if _, ok := confs[impName]; !ok {
			confs[impName] = make(map[string]string)
		}
		confs[impName][tag] = value
	}
	for impName, conf := range confs {
		imp, err := NewImporter(ctx, conf)
		if err != nil {
			return nil, fmt.Errorf("failed to create importer %s: %w", impName, err)
		}
		importers = append(importers, imp)
	}
	if len(importers) == 0 { // Not sure if this is needed, but let's be safe
		return nil, fmt.Errorf("no importers found")
	}
	if len(importers) == 1 { // If there's only one importer, return it directly for performance
		return importers[0], nil
	}

	return &MultiImporter{
		Importers: importers,
		name:      name,
	}, nil
}

// Origin returns the origin of the Idx importer.
func (m *MultiImporter) Origin() string {
	return m.name
}

// Type returns the type of the Idx importer.
func (m *MultiImporter) Type() string {
	return "multi"
}

// Root returns the root of the Idx importer.
func (m *MultiImporter) Root() string {
	return ""
}

// Scan returns a combined channel of scan results from all Importers.
func (m *MultiImporter) Scan() (<-chan *ScanResult, error) {

	scanChan := make(chan *ScanResult)
	var wg sync.WaitGroup

	for i, imp := range m.Importers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			subChan, err := imp.Scan()
			if err != nil {
				scanChan <- &ScanResult{Error: &ScanError{Err: err}}
				return
			}
			for {
				result, ok := <-subChan
				if !ok {
					break // SubChannel is closed
				}
				if result.Record != nil {
					result.Record.Source = i + 1 // Set the source to the index of the importer
				}

				scanChan <- result
			}
		}()
	}

	go func() {
		wg.Wait()
		close(scanChan)
	}()

	return scanChan, nil
}

// Close closes all Importers.
func (m *MultiImporter) Close() error {
	var err error
	for _, imp := range m.Importers {
		if closeErr := imp.Close(); closeErr != nil {
			err = closeErr
		}
	}
	return err
}
