package peer

import (
	pb "redisClusterManager/proto"
	"context"
	"fmt"
	"log"
	"net"
	"runtime/debug"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// CacheService is the callback interface the gRPC server needs to serve cache requests.
// It avoids importing the src package from peer (circular dependency).
type CacheService interface {
	Get(group, key string) ([]byte, error)
	Delete(group, key string)
	PushReplica(group, key string, value []byte)
}

// GrpcServer wraps a gRPC server that handles incoming cache requests.
type GrpcServer struct {
	server  *grpc.Server
	addr    string
	service CacheService
}

// NewGrpcServer creates a gRPC server for cache operations.
func NewGrpcServer(addr string, service CacheService) *GrpcServer {
	s := &GrpcServer{
		addr:    addr,
		service: service,
	}
	return s
}

// Start begins serving gRPC on the configured address.
// The serveErr channel receives any fatal error from the gRPC server.
func (s *GrpcServer) Start(tlsCfg TLSConfig) (<-chan error, error) {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return nil, err
	}

	creds, err := ServerCredentials(tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("tls server credentials: %w", err)
	}

	s.server = grpc.NewServer(
		grpc.Creds(creds),
		grpc.MaxConcurrentStreams(256),
		grpc.ChainUnaryInterceptor(
			recoveryInterceptor(),
			loggingInterceptor(),
		),
	)
	pb.RegisterGroupCacheServer(s.server, &cacheServiceAdapter{s.service})
	reflection.Register(s.server)

	serveErr := make(chan error, 1)
	go func() {
		if err := s.server.Serve(lis); err != nil {
			serveErr <- err
		}
		close(serveErr)
	}()
	return serveErr, nil
}

// Stop gracefully stops the gRPC server.
func (s *GrpcServer) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
}

// cacheServiceAdapter bridges CacheService to the generated GroupCacheServer interface.
type cacheServiceAdapter struct {
	svc CacheService
}

func (a *cacheServiceAdapter) Get(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	value, err := a.svc.Get(req.GetGroup(), req.GetKey())
	if err != nil {
		return nil, err
	}
	return &pb.Response{Value: value}, nil
}

func (a *cacheServiceAdapter) Invalidate(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	a.svc.Delete(req.GetGroup(), req.GetKey())
	return &pb.Response{}, nil
}

func (a *cacheServiceAdapter) Push(ctx context.Context, req *pb.PushRequest) (*pb.Response, error) {
	a.svc.PushReplica(req.GetGroup(), req.GetKey(), req.GetValue())
	return &pb.Response{}, nil
}

// --- gRPC Interceptors ---

// recoveryInterceptor catches panics in gRPC handlers and converts them to errors.
func recoveryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[gRPC] PANIC in %s: %v\n%s", info.FullMethod, r, string(debug.Stack()))
				err = status.Errorf(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}

// loggingInterceptor logs each gRPC request with method, duration, and error.
func loggingInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		start := time.Now()
		resp, err = handler(ctx, req)
		duration := time.Since(start)
		if err != nil {
			log.Printf("[gRPC] %s failed in %v: %v", info.FullMethod, duration, err)
		}
		return resp, err
	}
}
