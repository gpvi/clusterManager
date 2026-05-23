// gRPC service stubs for GroupCache.
// Manually written to avoid protoc-gen-go-grpc dependency.
// Equivalent to what protoc-gen-go-grpc generates for:
//
//   service GroupCache {
//     rpc Get(Request) returns (Response);
//     rpc Invalidate(Request) returns (Response);
//     rpc Push(PushRequest) returns (Response);
//   }

package proto

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ========== Client ==========

// PushRequest carries a full cache entry for replication.
type PushRequest struct {
	Group string `protobuf:"bytes,1,opt,name=group,proto3" json:"group,omitempty"`
	Key   string `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	Value []byte `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *PushRequest) Reset()         { *x = PushRequest{} }
func (x *PushRequest) String() string { return "PushRequest{" + x.Group + "/" + x.Key + "}" }
func (*PushRequest) ProtoMessage()    {}
func (x *PushRequest) GetGroup() string {
	if x != nil { return x.Group }; return ""
}
func (x *PushRequest) GetKey() string {
	if x != nil { return x.Key }; return ""
}
func (x *PushRequest) GetValue() []byte {
	if x != nil { return x.Value }; return nil
}

// GroupCacheClient is the client API for GroupCache service.
type GroupCacheClient interface {
	Get(ctx context.Context, in *Request, opts ...grpc.CallOption) (*Response, error)
	Invalidate(ctx context.Context, in *Request, opts ...grpc.CallOption) (*Response, error)
	Push(ctx context.Context, in *PushRequest, opts ...grpc.CallOption) (*Response, error)
}

type groupCacheClient struct {
	cc grpc.ClientConnInterface
}

// NewGroupCacheClient creates a gRPC client for the GroupCache service.
func NewGroupCacheClient(cc grpc.ClientConnInterface) GroupCacheClient {
	return &groupCacheClient{cc: cc}
}

func (c *groupCacheClient) Get(ctx context.Context, in *Request, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, "/proto.GroupCache/Get", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *groupCacheClient) Invalidate(ctx context.Context, in *Request, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, "/proto.GroupCache/Invalidate", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *groupCacheClient) Push(ctx context.Context, in *PushRequest, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, "/proto.GroupCache/Push", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ========== Server ==========

// GroupCacheServer is the server API for GroupCache service.
type GroupCacheServer interface {
	Get(ctx context.Context, req *Request) (*Response, error)
	Invalidate(ctx context.Context, req *Request) (*Response, error)
	Push(ctx context.Context, req *PushRequest) (*Response, error)
}

// UnimplementedGroupCacheServer may be embedded for forward compatibility.
type UnimplementedGroupCacheServer struct{}

func (UnimplementedGroupCacheServer) Get(context.Context, *Request) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}
func (UnimplementedGroupCacheServer) Invalidate(context.Context, *Request) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Invalidate not implemented")
}
func (UnimplementedGroupCacheServer) Push(context.Context, *PushRequest) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Push not implemented")
}

// UnsafeGroupCacheServer allows disabling forward compatibility checks.
type UnsafeGroupCacheServer interface {
	mustEmbedUnimplementedGroupCacheServer()
}

// RegisterGroupCacheServer registers the service implementation with a gRPC server.
func RegisterGroupCacheServer(s grpc.ServiceRegistrar, srv GroupCacheServer) {
	s.RegisterService(&GroupCache_ServiceDesc, srv)
}

// ========== Handlers ==========

func _GroupCache_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Request)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GroupCacheServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.GroupCache/Get",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GroupCacheServer).Get(ctx, req.(*Request))
	}
	return interceptor(ctx, in, info, handler)
}

func _GroupCache_Invalidate_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Request)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GroupCacheServer).Invalidate(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.GroupCache/Invalidate",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GroupCacheServer).Invalidate(ctx, req.(*Request))
	}
	return interceptor(ctx, in, info, handler)
}

func _GroupCache_Push_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PushRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GroupCacheServer).Push(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.GroupCache/Push",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GroupCacheServer).Push(ctx, req.(*PushRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// GroupCache_ServiceDesc is the gRPC service descriptor.
var GroupCache_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "proto.GroupCache",
	HandlerType: (*GroupCacheServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Get",
			Handler:    _GroupCache_Get_Handler,
		},
		{
			MethodName: "Invalidate",
			Handler:    _GroupCache_Invalidate_Handler,
		},
		{
			MethodName: "Push",
			Handler:    _GroupCache_Push_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/cache.proto",
}
