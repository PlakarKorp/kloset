package importer

import (
	"sync"

	"github.com/PlakarKorp/kloset/kcontext"
)

// MultiImporter combines multiple Importers into one.
type MultiImporter struct {
	Importers []Importer
	offset    int
}

// NewMultiImporter creates a new MultiImporter with the given Importers.
func NewMultiImporter(ctx *kcontext.KContext, sources []Importer, offset int) (Importer, error) {
	return &MultiImporter{
		Importers: sources,
		offset:    offset,
	}, nil
}

// Origin returns the origin of the Idx importer.
func (m *MultiImporter) Origin() string {
	orig := "multi-"
	for _, imp := range m.Importers {
		orig += imp.Origin() + "-"
	}

	return orig
}

// Type returns the type of the Idx importer.
func (m *MultiImporter) Type() string {
	return "multi"
}

// Root returns the root of the Idx importer.
func (m *MultiImporter) Root() string {
	return "/"
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
				scanChan <- &ScanResult{Error: &ScanError{Err: err, Source: i + m.offset}}
				return
			}
			for {
				result, ok := <-subChan
				if !ok {
					break // SubChannel is closed
				}
				if result.Record != nil {
					result.Record.Source = i + m.offset // Set the source to the index of the importer
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
