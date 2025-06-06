package importer

import (
	"bytes"
	"context"
	"fmt"
	"github.com/PlakarKorp/kloset/objects"
	grpc_importer "github.com/PlakarKorp/kloset/snapshot/importer/pkg"
	"io"
	"io/fs"
	"log"
)

type GrpcImporter struct {
	GrpcClientScan   grpc_importer.ImporterClient //used to Scan (but also to get Origin, Type, Root and Close)
	GrpcClientReader grpc_importer.ImporterClient //TODO: add a list of ClientReader //used to Open/Read/Close files
}

func (g *GrpcImporter) Origin() string {
	info, err := g.GrpcClientScan.Info(context.Background(), &grpc_importer.InfoRequest{})
	if err != nil {
		return ""
	}
	return info.GetOrigin()
}

func (g *GrpcImporter) Type() string {
	info, err := g.GrpcClientScan.Info(context.Background(), &grpc_importer.InfoRequest{})
	if err != nil {
		return ""
	}
	return info.GetType()
}

func (g *GrpcImporter) Root() string {
	info, err := g.GrpcClientScan.Info(context.Background(), &grpc_importer.InfoRequest{})
	if err != nil {
		return ""
	}
	return info.GetRoot()
}

//===========

type GrpcReader struct {
	client grpc_importer.ImporterClient
	stream grpc_importer.Importer_OpenClient
	path   string
	buf    *bytes.Buffer
}

func NewGrpcReader(client grpc_importer.ImporterClient, path string) *GrpcReader {
	return &GrpcReader{
		client: client,
		buf:    bytes.NewBuffer(nil),
		path:   path,
	}
}

func (g *GrpcReader) Read(p []byte) (n int, err error) {
	if g.buf.Len() != 0 {
		n, err = g.buf.Read(p)
		if n > 0 || err != nil {
			return n, err
		}
	}
	if g.stream == nil {
		g.stream, err = g.client.Open(context.Background(), &grpc_importer.OpenRequest{
			Pathname: g.path,
		})
		if err != nil {
			return 0, fmt.Errorf("failed to open file %s: %w", g.path, err)
		}
	}
	fileResponse, err := g.stream.Recv()
	if err != nil {
		if err == io.EOF {
			return 0, io.EOF
		}
		return 0, fmt.Errorf("failed to receive file data: %w", err)
	}
	if fileResponse.GetChunk() != nil {
		g.buf.Write(fileResponse.GetChunk())
		n, err = g.buf.Read(p)
		if n > 0 || err != nil {
			return n, err
		}
	}
	return 0, fmt.Errorf("unexpected response: %v", fileResponse)
}

func (g *GrpcReader) Close() error {
	_, err := g.client.Close(context.Background(), &grpc_importer.CloseRequest{
		Pathname: g.path,
	})
	if err != nil {
		return fmt.Errorf("failed to close record %s: %w", g.path, err)
	}
	return nil
}

//===========

func (g *GrpcImporter) Scan() (<-chan *ScanResult, error) {
	stream, err := g.GrpcClientScan.Scan(context.Background(), &grpc_importer.ScanRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to start scan: %w", err)
	}

	results := make(chan *ScanResult, 1000)
	go func() {
		defer close(results)

		for {
			log.Printf("Waiting for scan response...\n")
			response, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					break
				}
				results <- NewScanError("", fmt.Errorf("failed to receive scan response: %w", err))
			}
			isXattr := false
			if response.GetRecord().GetXattr() != nil {
				isXattr = true
			}

			if response.GetRecord() != nil {
				//log.Printf("Received record for pathname: %s\n", response.GetPathname())

				results <- &ScanResult{
					Record: &ScanRecord{
						Pathname: response.GetPathname(),
						Reader: NewLazyReader(func() (io.ReadCloser, error) {
							return NewGrpcReader(g.GrpcClientReader, response.GetPathname()), nil //TODO: check to not make to much files
						}),
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
			} else if response.GetError() != nil {
				results <- NewScanError(response.GetPathname(), fmt.Errorf("scan error: %s", response.GetError().GetMessage()))
			} else {
				results <- NewScanError("", fmt.Errorf("unexpected response: %v", response))
			}
			log.Printf("End of loop\n")
		}
	}()
	//log.Printf("Scan completed, closing results channel\n")
	return results, nil
}

func (g *GrpcImporter) NewExtendedAttributeReader(path string, xattr string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}

func (g *GrpcImporter) Close() error {
	if g.GrpcClientScan != nil {
		if conn, ok := g.GrpcClientScan.(interface{ Close() error }); ok {
			return conn.Close()
		}
	}
	return nil
}
