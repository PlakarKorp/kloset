package importer

import (
	"context"
	"fmt"
	"io"
	"os"

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

	results := make(chan *ScanResult, 100)
	go func() {
		defer close(results)
		for {
			response, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					fmt.Fprintf(os.Stderr, "Scan completed successfully.\n")
					break
				}
				results <- NewScanError("", fmt.Errorf("failed to receive scan response: %w", err))
				return
			}
			if response.GetError() != nil {
				results <- NewScanError(response.GetPathname(), fmt.Errorf("scan error: %s", response.GetError().GetMessage()))
				continue
			}
			if response.GetRecord() == nil {
				results <- NewScanError(response.GetPathname(), fmt.Errorf("scan error: no record"))
				continue
			}
			isXattr := false
			if response.GetRecord().GetXattr() != nil {
				isXattr = true
			}
			results <- &ScanResult{
				Record: &ScanRecord{
					Pathname: response.GetPathname(),
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
		}
	}()
	return results, nil
}

func (g *GrpcImporter) NewReader(path string) (io.ReadCloser, error) {
	stream, err := g.GrpcClient.Read(context.Background(), &grpc_importer.ReadRequest{
		Pathname: path,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start read stream: %w", err)
	}

	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()
		for {
			resp, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				pw.CloseWithError(fmt.Errorf("failed to receive read response: %w", err))
				return
			}
			if len(resp.GetData()) > 0 {
				_, werr := pw.Write(resp.GetData())
				if werr != nil {
					pw.CloseWithError(fmt.Errorf("failed to write to pipe: %w", werr))
					return
				}
			}
		}
	}()

	return pr, nil
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
