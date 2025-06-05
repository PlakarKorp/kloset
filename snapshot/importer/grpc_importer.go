package importer

import (
	"context"
	"fmt"
	"io"

	"io/fs"

	"github.com/PlakarKorp/kloset/objects"
	grpc_importer "github.com/PlakarKorp/kloset/snapshot/importer/pkg"
)

type GrpcImporter struct {
	GrpcClient grpc_importer.ImporterClient
}

func (g *GrpcImporter) Origin() string {
	info, err := g.GrpcClient.Info(context.Background(), &grpc_importer.InfoRequest{})
	if err != nil {
		return ""
	}
	return info.GetOrigin()
}

func (g *GrpcImporter) Type() string {
	info, err := g.GrpcClient.Info(context.Background(), &grpc_importer.InfoRequest{})
	if err != nil {
		return ""
	}
	return info.GetType()
}

func (g *GrpcImporter) Root() string {
	info, err := g.GrpcClient.Info(context.Background(), &grpc_importer.InfoRequest{})
	if err != nil {
		return ""
	}
	return info.GetRoot()
}

func (g *GrpcImporter) Scan() (<-chan *ScanResult, error) {
	stream, err := g.GrpcClient.Scan(context.Background(), &grpc_importer.ScanRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to start scan: %w", err)
	}

	results := make(chan *ScanResult, 1)
	go func() {
		defer close(results)

		var currentRecord *ScanResult
		var pipeReader *io.PipeReader
		var pipeWriter *io.PipeWriter

		for {
			response, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					break
				}
				results <- NewScanError("", fmt.Errorf("failed to receive scan response: %w", err))
				return
			}
			isXattr := false
			if response.GetRecord().GetXattr() != nil {
				isXattr = true
			}

        	if response.GetRecord() != nil {
				fmt.Printf("Received record for pathname: %s\n", response.GetPathname())
				if pipeWriter != nil {
					fmt.Printf("Closing previous pipe writer for pathname: %s\n", currentRecord.Record.Pathname)
					pipeWriter.Close()
				}

				pipeReader, pipeWriter = io.Pipe()
				currentRecord = &ScanResult{
					Record: &ScanRecord{
						Pathname: response.GetPathname(),
						Reader: pipeReader,
						FileInfo: objects.FileInfo{
							Lname:      response.GetRecord().GetFileinfo().GetName(),
							Lsize:      response.GetRecord().GetFileinfo().GetSize(),
							Lmode:      fs.FileMode(response.GetRecord().GetFileinfo().GetMode()),
							LmodTime:   response.GetRecord().GetFileinfo().GetModTime().AsTime(),
							Ldev:       response.GetRecord().GetFileinfo().GetDev(),
							Lino:       response.GetRecord().GetFileinfo().GetIno(),
							Luid:       response.GetRecord().GetFileinfo().GetUid(),
							Lgid:       response.GetRecord().GetFileinfo().GetGid(),
							Lnlink:     uint16(response.GetRecord().GetFileinfo().GetNlink()),
							Lusername:  response.GetRecord().GetFileinfo().GetUsername(),
							Lgroupname: response.GetRecord().GetFileinfo().GetGroupname(),
						},
						Target:         response.GetRecord().Target,
						FileAttributes: response.GetRecord().GetFileAttributes(),
						IsXattr:        isXattr,
						XattrName:      response.GetRecord().GetXattr().GetName(),
						XattrType:      objects.Attribute(response.GetRecord().GetXattr().GetType()),
					},
					Error: nil,
				}
				fmt.Printf("check\n")
			} else if response.GetChunk() != nil {
				fmt.Printf("Received chunk for pathname: %s\n", response.GetPathname())
				if pipeWriter == nil {
					fmt.Printf("No pipe writer available for pathname: %s, creating new record\n", response.GetPathname())
				}
				if response.GetChunk().GetEof() != nil {
					results <- currentRecord
					pipeWriter.Close()
					return
				}
				_, err := pipeWriter.Write(response.GetChunk().GetData())
				if err != nil {
					pipeWriter.CloseWithError(err)
					return
				}
			} else if response.GetError() != nil {
				results <- NewScanError(response.GetPathname(), fmt.Errorf("scan error: %s", response.GetError().GetMessage()))
			} else {
				results <- NewScanError("", fmt.Errorf("unexpected response: %v", response))
			}
			fmt.Printf("End of loop\n")
    	}
		if pipeWriter != nil {
			pipeWriter.Close()
		}
	}()
	return results, nil
}

func (g *GrpcImporter) NewExtendedAttributeReader(path string, xattr string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}

func (g *GrpcImporter) Close() error {
	if g.GrpcClient != nil {
		if conn, ok := g.GrpcClient.(interface{ Close() error }); ok {
			return conn.Close()
		}
	}
	return nil
}
