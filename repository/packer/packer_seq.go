package packer

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"hash"
	"io"
	"math/big"
	randv2 "math/rand/v2"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/packfile"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
)

// ErrShutdown is returned by Put when Wait has been called. See
// PlakarKorp/plakar#2106.
var ErrShutdown = errors.New("packer: manager has been shut down")

type seqPackerManager struct {
	inflightMACs   map[resources.Type]*sync.Map
	packerChan     []chan PackerMsg
	packerChanDone chan struct{}

	// closing is closed by Wait to signal shutdown to both Run workers and
	// in-flight Put callers (which select on it instead of sending blindly).
	closing   chan struct{}
	closeOnce sync.Once

	packfileFactory packfile.PackfileCtor
	storageConf     *storage.Configuration
	encodingFunc    func(io.Reader) (io.Reader, error)
	hashFactory     func() hash.Hash
	appCtx          *kcontext.KContext
	nChan           int

	// XXX: Temporary hack callback-based to ease the transition diff.
	// To be revisited with either an interface or moving this file inside repository/
	flush func(packfile.Packfile) error
}

func NewSeqPackerManager(ctx *kcontext.KContext, storageConfiguration *storage.Configuration, encodingFunc func(io.Reader) (io.Reader, error), packfileFactory packfile.PackfileCtor, hashFactory func() hash.Hash, flusher func(packfile.Packfile) error) PackerManagerInt {
	inflightsMACs := make(map[resources.Type]*sync.Map)
	for _, Type := range resources.Types() {
		inflightsMACs[Type] = &sync.Map{}
	}

	// VFS entries dedicated channel
	nChan := ctx.MaxConcurrency + 1
	ret := &seqPackerManager{
		inflightMACs:    inflightsMACs,
		packerChan:      make([]chan PackerMsg, nChan),
		packerChanDone:  make(chan struct{}),
		closing:         make(chan struct{}),
		packfileFactory: packfileFactory,
		storageConf:     storageConfiguration,
		encodingFunc:    encodingFunc,
		hashFactory:     hashFactory,
		appCtx:          ctx,
		flush:           flusher,
		nChan:           nChan,
	}

	for i := range nChan {
		ret.packerChan[i] = make(chan PackerMsg)
	}

	return ret
}

func (mgr *seqPackerManager) Run() error {
	packerResultChan := make(chan packfile.Packfile, mgr.nChan)

	flusherGroup, _ := errgroup.WithContext(context.TODO())
	for range mgr.nChan {
		flusherGroup.Go(func() error {
			for pfile := range packerResultChan {
				if pfile == nil || pfile.Size() == 0 {
					continue
				}

				if err := mgr.flush(pfile); err != nil {
					err = fmt.Errorf("failed to flush packer: %w", err)

					// We are already in an error path, no need to account for more.
					_ = pfile.Cleanup()

					for range packerResultChan {
					}

					return err
				}

				for _, record := range pfile.Entries() {
					mgr.inflightMACs[record.Type].Delete(record.MAC)
				}

				// What should we do here? I believe it shouldn't be a hard
				// error, failing to cleanup resources isn't going to break
				// the Backup.
				_ = pfile.Cleanup()
			}
			return nil
		})
	}

	// This needs to be context.Background (OR TODO for that matter), because
	// it's a real worked / background task it's not tied to the backup
	// pipeline and as such we might have valid packerManager.Put() happening
	// while this background task was ripped under our feet.
	workerGroup, workerCtx := errgroup.WithContext(context.TODO())
	for i := range mgr.nChan {
		workerGroup.Go(func() error {
			var pfile packfile.Packfile

			// flushTail finalizes the in-flight packfile, if any, before
			// the worker returns.
			flushTail := func() error {
				if pfile != nil && pfile.Size() > 0 {
					if err := mgr.AddPadding(pfile, int(mgr.storageConf.Chunking.MinSize)); err != nil {
						return err
					}
					packerResultChan <- pfile
				}
				return nil
			}

			for {
				select {
				case <-workerCtx.Done():
					return workerCtx.Err()
				case <-mgr.closing:
					return flushTail()
				case pm, ok := <-mgr.packerChan[i]:
					if !ok {
						return flushTail()
					}

					if pfile == nil {
						var err error
						pfile, err = mgr.packfileFactory(mgr.hashFactory)
						if err != nil {
							return err
						}

						if err := mgr.AddPadding(pfile, int(mgr.storageConf.Chunking.MinSize)); err != nil {
							return err
						}
					}

					pm.Flags &^= PackfileHot
					if err := pfile.AddBlob(pm.Type, pm.Version, pm.MAC, pm.Data, uint32(pm.Flags)); err != nil {
						return err
					}

					if pfile.Size() > mgr.storageConf.Packfile.MaxSize {
						if err := mgr.AddPadding(pfile, int(mgr.storageConf.Chunking.MinSize)); err != nil {
							return err
						}

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
		mgr.appCtx.Cancel(err)
	}

	// Close the result channel and wait for the flusher to finish.
	close(packerResultChan)
	if err := flusherGroup.Wait(); err != nil {
		mgr.appCtx.GetLogger().Error("Flusher group error: %s", err)
		mgr.appCtx.Cancel(err)
	}

	mgr.packerChanDone <- struct{}{}
	close(mgr.packerChanDone)
	return nil
}

func (mgr *seqPackerManager) Wait() {
	// closeOnce makes Wait safe to call more than once; some error paths in
	// snapshot.Backup do call it twice.
	mgr.closeOnce.Do(func() { close(mgr.closing) })
	<-mgr.packerChanDone
}

func (mgr *seqPackerManager) InsertIfNotPresent(Type resources.Type, mac objects.MAC) (bool, error) {
	if _, exists := mgr.inflightMACs[Type].LoadOrStore(mac, struct{}{}); exists {
		// tell prom exporter that we collided a blob
		return true, nil
	}

	return false, nil
}

func (mgr *seqPackerManager) Put(hint int, Type resources.Type, mac objects.MAC, data []byte, hot bool) error {
	// Skip encoding work if shutdown was already signalled.
	select {
	case <-mgr.closing:
		return ErrShutdown
	default:
	}

	encodedReader, err := mgr.encodingFunc(bytes.NewReader(data))
	if err != nil {
		return err
	}
	encoded, err := io.ReadAll(encodedReader)
	if err != nil {
		return err
	}

	var i int
	switch {
	case Type != resources.RT_CHUNK:
		i = mgr.nChan - 1
	case hint == -1:
		i = randv2.IntN(mgr.nChan - 1)
	default:
		i = hint
	}

	var f PackerMsgFlags
	if hot {
		f |= PackfileHot
	}

	msg := PackerMsg{Type: Type, Version: versioning.GetCurrentVersion(Type), Timestamp: time.Now(), MAC: mac, Data: encoded, Flags: f}
	select {
	case mgr.packerChan[i] <- msg:
		return nil
	case <-mgr.closing:
		return ErrShutdown
	}
}

func (mgr *seqPackerManager) Exists(Type resources.Type, mac objects.MAC) (bool, error) {
	if _, exists := mgr.inflightMACs[Type].Load(mac); exists {
		return true, nil
	}

	return false, nil
}

func (mgr *seqPackerManager) AddPadding(packfile packfile.Packfile, maxSize int) error {
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

	return packfile.AddBlob(resources.RT_RANDOM, versioning.GetCurrentVersion(resources.RT_RANDOM), mac, buffer, 0)
}
