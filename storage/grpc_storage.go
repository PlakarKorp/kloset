/*
 * Copyright (c) 2021 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package storage

import (
	"bytes"
	"context"
	"fmt"
	"github.com/PlakarKorp/kloset/objects"
	grpc_storage "github.com/PlakarKorp/kloset/storage/pkg"
	"io"
)

type GrpcStorage struct {
	GrpcClient grpc_storage.StoreClient
}

const bufferSize = 4096

func NewGrpcStorage(client grpc_storage.StoreClient) *GrpcStorage {
	return &GrpcStorage{
		GrpcClient: client,
	}
}

func (s *GrpcStorage) Create(ctx context.Context, config []byte) error {
	_, err := s.GrpcClient.Create(ctx, &grpc_storage.CreateRequest{Config: config})
	if err != nil {
		return err
	}
	return nil
}

func (s *GrpcStorage) Open(ctx context.Context) ([]byte, error) {
	resp, err := s.GrpcClient.Open(ctx, &grpc_storage.OpenRequest{})
	if err != nil {
		return nil, err
	}
	return resp.Config, nil
}

func (s *GrpcStorage) Location() string {
	resp, err := s.GrpcClient.GetLocation(context.Background(), &grpc_storage.GetLocationRequest{})
	if err != nil {
		return ""
	}
	return resp.Location
}

func (s *GrpcStorage) Mode() Mode {
	resp, err := s.GrpcClient.GetMode(context.Background(), &grpc_storage.GetModeRequest{})
	if err != nil {
		return Mode(0)
	}
	var mode Mode
	switch resp.Mode {
		case grpc_storage.Mode_MODE_READ_WRITE:
			mode = ModeWrite
		case grpc_storage.Mode_MODE_READ_ONLY:
			mode = ModeRead
		default:
			mode = Mode(0)
	}
	return mode
}

func (s *GrpcStorage) Size() (int64) {
	resp, err := s.GrpcClient.GetSize(context.Background(), &grpc_storage.GetSizeRequest{})
	if err != nil {
		return -1
	}
	return resp.Size
}

//========================

type chunkSenderFunc func(chunk []byte) error

func sendChunks(rd io.Reader, send chunkSenderFunc) (int64, error) {
	buffer := make([]byte, bufferSize)
	var totalBytes int64

	for {
		n, err := rd.Read(buffer)
		if n > 0 {
			if sendErr := send(buffer[:n]); sendErr != nil {
				return totalBytes, fmt.Errorf("failed to send chunk: %w", sendErr)
			}
			totalBytes += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return totalBytes, fmt.Errorf("failed to read: %w", err)
		}
	}
	return totalBytes, nil
}

type chunkReceiverFunc func() ([]byte, error)

func receiveChunks(recv chunkReceiverFunc) (io.Reader, error) {
	var buf bytes.Buffer

	for {
		chunk, err := recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("receive chunk: %w", err)
		}
		buf.Write(chunk)
	}
	return io.NopCloser(&buf), nil
}

func toGrpcMAC(mac objects.MAC) *grpc_storage.MAC {
	return &grpc_storage.MAC{Value: mac[:]}
}

//========================

func (s *GrpcStorage) GetStates() ([]objects.MAC, error) {
	resp, err := s.GrpcClient.GetStates(context.Background(), &grpc_storage.GetStatesRequest{})
	if err != nil {
		return nil, err
	}

	var states []objects.MAC
	for _, mac := range resp.Macs {
		states = append(states, objects.MAC(mac.Value))
	}
	return states, nil
}

func (s *GrpcStorage) PutState(mac objects.MAC, rd io.Reader) (int64, error) {
	stream, err := s.GrpcClient.PutState(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to start PutState stream: %w", err)
	}
	defer stream.CloseSend()

	return sendChunks(rd, func(chunk []byte) error {
		return stream.Send(&grpc_storage.PutStateRequest{
			Mac:   toGrpcMAC(mac),
			Chunk: chunk,
		})
	})
}

func (s *GrpcStorage) GetState(mac objects.MAC) (io.Reader, error) {
	stream, err := s.GrpcClient.GetState(context.Background(), &grpc_storage.GetStateRequest{
		Mac: toGrpcMAC(mac),
	})
	if err != nil {
		return nil, fmt.Errorf("get state: %w", err)
	}

	return receiveChunks(func() ([]byte, error) {
		resp, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		return resp.Chunk, nil
	})
}

func (s *GrpcStorage) DeleteState(mac objects.MAC) error {
	_, err := s.GrpcClient.DeleteState(context.Background(), &grpc_storage.DeleteStateRequest{
		Mac: toGrpcMAC(mac),
	})
	if err != nil {
		return fmt.Errorf("failed to delete state: %w", err)
	}
	return nil
}

func (s *GrpcStorage) GetPackfiles() ([]objects.MAC, error) {
	resp, err := s.GrpcClient.GetPackfiles(context.Background(), &grpc_storage.GetPackfilesRequest{})
	if err != nil {
		return nil, err
	}

	var packfiles []objects.MAC
	for _, mac := range resp.Macs {
		packfiles = append(packfiles, objects.MAC(mac.Value))
	}
	return packfiles, nil
}

func (s *GrpcStorage) PutPackfile(mac objects.MAC, rd io.Reader) (int64, error) {
	stream, err := s.GrpcClient.PutPackfile(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to start PutPackfile stream: %w", err)
	}
	defer stream.CloseSend()

	return sendChunks(rd, func(chunk []byte) error {
		return stream.Send(&grpc_storage.PutPackfileRequest{
			Mac:   toGrpcMAC(mac),
			Chunk: chunk,
		})
	})
}

func (s *GrpcStorage) GetPackfile(mac objects.MAC) (io.Reader, error) {
	stream, err := s.GrpcClient.GetPackfile(context.Background(), &grpc_storage.GetPackfileRequest{
		Mac: toGrpcMAC(mac),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get packfile: %w", err)
	}

	return receiveChunks(func() ([]byte, error) {
		resp, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		return resp.Chunk, nil
	})
}

func (s *GrpcStorage) GetPackfileBlob(mac objects.MAC, offset uint64, length uint32) (io.Reader, error) {
	stream, err := s.GrpcClient.GetPackfileBlob(context.Background(), &grpc_storage.GetPackfileBlobRequest{
		Mac:    toGrpcMAC(mac),
		Offset: offset,
		Length: length,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get packfile blob: %w", err)
	}

	return receiveChunks(func() ([]byte, error) {
		resp, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		return resp.Chunk, nil
	})
}

func (s *GrpcStorage) DeletePackfile(mac objects.MAC) error {
	_, err := s.GrpcClient.DeletePackfile(context.Background(), &grpc_storage.DeletePackfileRequest{
		Mac: toGrpcMAC(mac),
	})
	if err != nil {
		return fmt.Errorf("failed to delete packfile: %w", err)
	}
	return nil
}

func (s *GrpcStorage) GetLocks() ([]objects.MAC, error) {
	resp, err := s.GrpcClient.GetLocks(context.Background(), &grpc_storage.GetLocksRequest{})
	if err != nil {
		return nil, err
	}

	var locks []objects.MAC
	for _, mac := range resp.Macs {
		locks = append(locks, objects.MAC(mac.Value))
	}
	return locks, nil
}

func (s *GrpcStorage) PutLock(lockID objects.MAC, rd io.Reader) (int64, error) {
	stream, err := s.GrpcClient.PutLock(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to start PutLock stream: %w", err)
	}
	defer stream.CloseSend()

	return sendChunks(rd, func(chunk []byte) error {
		return stream.Send(&grpc_storage.PutLockRequest{
			Mac:   toGrpcMAC(lockID),
			Chunk: chunk,
		})
	})
}

func (s *GrpcStorage) GetLock(lockID objects.MAC) (io.Reader, error) {
	stream, err := s.GrpcClient.GetLock(context.Background(), &grpc_storage.GetLockRequest{
		Mac: toGrpcMAC(lockID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get lock: %w", err)
	}

	return receiveChunks(func() ([]byte, error) {
		resp, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		return resp.Chunk, nil
	})
}

func (s *GrpcStorage) DeleteLock(lockID objects.MAC) error {
	_, err := s.GrpcClient.DeleteLock(context.Background(), &grpc_storage.DeleteLockRequest{
		Mac: toGrpcMAC(lockID),
	})
	if err != nil {
		return fmt.Errorf("failed to delete lock: %w", err)
	}
	return nil
}

func (s *GrpcStorage) Close() error {
	_, err := s.GrpcClient.Close(context.Background(), &grpc_storage.CloseRequest{})
	if err != nil {
		return err
	}
	return nil
}
