package packer

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"hash"
	"io"
	"math/big"
	"runtime"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/packfile"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/storage"
	"github.com/PlakarKorp/kloset/versioning"
)

func init() {
	// used for padding random bytes
	versioning.Register(resources.RT_RANDOM, versioning.FromString("1.0.0"))
}

type PackerManagerInt interface {
	Run() error
	Wait()
	InsertIfNotPresent(Type resources.Type, mac objects.MAC) (bool, error)
	Put(Type resources.Type, mac objects.MAC, data []byte) error
	Exists(Type resources.Type, mac objects.MAC) (bool, error)
}

type packerManager struct {
	inflightMACs   map[resources.Type]*sync.Map
	packerChan     chan PackerMsg
	packerChanDone chan struct{}

	storageConf  *storage.Configuration
	encodingFunc func(io.Reader) (io.Reader, error)
	hashFactory  func() hash.Hash
	appCtx       *kcontext.KContext

	// XXX: Temporary hack callback-based to ease the transition diff.
	// To be revisited with either an interface or moving this file inside repository/
	flush func(*packfile.PackFile) error
}

func NewPackerManager(ctx *kcontext.KContext, storageConfiguration *storage.Configuration, encodingFunc func(io.Reader) (io.Reader, error), hashFactory func() hash.Hash, flusher func(*packfile.PackFile) error) PackerManagerInt {
	inflightsMACs := make(map[resources.Type]*sync.Map)
	for _, Type := range resources.Types() {
		inflightsMACs[Type] = &sync.Map{}
	}
	return &packerManager{
		inflightMACs:   inflightsMACs,
		packerChan:     make(chan PackerMsg, runtime.NumCPU()*2+1),
		packerChanDone: make(chan struct{}),
		storageConf:    storageConfiguration,
		encodingFunc:   encodingFunc,
		hashFactory:    hashFactory,
		appCtx:         ctx,
		flush:          flusher,
	}
}

func (mgr *packerManager) Run() error {
	concurrency := runtime.NumCPU()*2 + 1

	packerResultChan := make(chan *packfile.PackFile, concurrency)

	flusherGroup, _ := errgroup.WithContext(context.TODO())
	for range concurrency {
		flusherGroup.Go(func() error {
			for pfile := range packerResultChan {
				if pfile == nil || pfile.Size() == 0 {
					continue
				}

				mgr.AddPadding(pfile, int(mgr.storageConf.Chunking.MinSize))

				if err := mgr.flush(pfile); err != nil {
					err = fmt.Errorf("failed to flush packer: %w", err)
					for range packerResultChan {
					}

					return err
				}

				for _, record := range pfile.Index {
					mgr.inflightMACs[record.Type].Delete(record.MAC)
				}
			}
			return nil
		})
	}

	// This needs to be context.Background (OR TODO for that matter), because
	// it's a real worked / background task it's not tied to the backup
	// pipeline and as such we might have valid packerManager.Put() happening
	// while this background task was ripped under our feet.
	workerGroup, workerCtx := errgroup.WithContext(context.TODO())
	for range concurrency {
		workerGroup.Go(func() error {
			var pfile *packfile.PackFile

			for {
				select {
				case <-workerCtx.Done():
					return workerCtx.Err()
				case pm, ok := <-mgr.packerChan:
					if !ok {
						if pfile != nil && pfile.Size() > 0 {
							packerResultChan <- pfile
						}
						return nil
					}

					if pfile == nil {
						pfile = packfile.New(mgr.hashFactory())
						mgr.AddPadding(pfile, int(mgr.storageConf.Chunking.MinSize))
					}

					pfile.AddBlob(pm.Type, pm.Version, pm.MAC, pm.Data, pm.Flags)

					if pfile.Size() > uint32(mgr.storageConf.Packfile.MaxSize) {
						packerResultChan <- pfile
						pfile = nil
					}
				}
			}
		})
	}

	// Wait for workers to finish.
	if err := workerGroup.Wait(); err != nil {
		mgr.appCtx.GetLogger().Error("Worker group error: %s", err)
	}

	// Close the result channel and wait for the flusher to finish.
	close(packerResultChan)
	if err := flusherGroup.Wait(); err != nil {
		mgr.appCtx.GetLogger().Error("Flusher group error: %s", err)
	}

	// Signal completion.
	mgr.packerChanDone <- struct{}{}
	close(mgr.packerChanDone)
	return nil
}

func (mgr *packerManager) Wait() {
	close(mgr.packerChan)
	<-mgr.packerChanDone
}

func (mgr *packerManager) InsertIfNotPresent(Type resources.Type, mac objects.MAC) (bool, error) {
	if _, exists := mgr.inflightMACs[Type].LoadOrStore(mac, struct{}{}); exists {
		// tell prom exporter that we collided a blob
		return true, nil
	}

	return false, nil
}

func (mgr *packerManager) Put(Type resources.Type, mac objects.MAC, data []byte) error {
	if encodedReader, err := mgr.encodingFunc(bytes.NewReader(data)); err != nil {
		return err
	} else {
		encoded, err := io.ReadAll(encodedReader)
		if err != nil {
			return err
		}

		mgr.packerChan <- PackerMsg{Type: Type, Version: versioning.GetCurrentVersion(Type), Timestamp: time.Now(), MAC: mac, Data: encoded}
		return nil
	}
}

func (mgr *packerManager) Exists(Type resources.Type, mac objects.MAC) (bool, error) {
	if _, exists := mgr.inflightMACs[Type].Load(mac); exists {
		return true, nil
	}

	return false, nil
}

type PackerMsg struct {
	Timestamp time.Time
	Type      resources.Type
	Version   versioning.Version
	MAC       objects.MAC
	Data      []byte
	Flags     uint32
}

type Packer struct {
	Blobs    map[resources.Type]map[[32]byte][]byte
	Packfile *packfile.PackFile
}

func NewPacker(hasher hash.Hash) *Packer {
	blobs := make(map[resources.Type]map[[32]byte][]byte)
	for _, Type := range resources.Types() {
		blobs[Type] = make(map[[32]byte][]byte)
	}
	return &Packer{
		Packfile: packfile.New(hasher),
		Blobs:    blobs,
	}
}

func (mgr *packerManager) AddPadding(packfile *packfile.PackFile, maxSize int) error {
	if maxSize < 0 {
		return fmt.Errorf("invalid padding size")
	}
	if maxSize == 0 {
		return nil
	}

	n, err := rand.Int(rand.Reader, big.NewInt(int64(maxSize)-1))
	if err != nil {
		return err
	}
	paddingSize := uint32(n.Uint64()) + 1

	buffer := make([]byte, paddingSize)
	_, err = rand.Read(buffer)
	if err != nil {
		return fmt.Errorf("failed to generate random padding: %w", err)
	}

	mac := objects.MAC{}
	_, err = rand.Read(mac[:])
	if err != nil {
		return fmt.Errorf("failed to generate random padding MAC: %w", err)
	}

	packfile.AddBlob(resources.RT_RANDOM, versioning.GetCurrentVersion(resources.RT_RANDOM), mac, buffer, 0)
	return nil
}

func (packer *Packer) addBlobIfNotExists(Type resources.Type, version versioning.Version, mac [32]byte, data []byte, flags uint32) bool {
	if _, ok := packer.Blobs[Type]; !ok {
		packer.Blobs[Type] = make(map[[32]byte][]byte)
	}
	if _, ok := packer.Blobs[Type][mac]; ok {
		return false
	}
	packer.Blobs[Type][mac] = data
	packer.Packfile.AddBlob(Type, version, mac, data, flags)
	return true
}

func (packer *Packer) size() uint32 {
	return packer.Packfile.Size()
}

func (packer *Packer) Types() []resources.Type {
	ret := make([]resources.Type, 0, len(packer.Blobs))
	for k := range packer.Blobs {
		ret = append(ret, k)
	}
	return ret
}
