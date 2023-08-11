package grpc_kubernetes_resolver

import (
	"google.golang.org/grpc/grpclog"
	grpc_rosover "google.golang.org/grpc/resolver"
	"strings"
)

import (
	"context"
)

const scheme = "endpoints"

type endpointBuilder struct {
}

// Build creates a new resolver for the given target.
//
// gRPC dial calls Build synchronously, and fails if the returned error is
// not nil.
func (builder *endpointBuilder) Build(target grpc_rosover.Target, cc grpc_rosover.ClientConn, opts grpc_rosover.BuildOptions) (grpc_rosover.Resolver, error) {
	name := target.URL.Host
	namespace := strings.Trim(strings.ReplaceAll(target.URL.Path, "/", ""), " ")
	if namespace == "" {
		namespace = "default"
	}
	grpclog.Infof("[grpc-kubernetes-resolver] start watch endpoint namespace:'%s',name:'%s' ", namespace, name)

	ctx, cancelFunc := context.WithCancel(context.Background())
	pipe := make(chan []string)
	go endpointNetworkWatch(ctx, namespace, name, pipe)
	go populateTarget(ctx, cc, pipe)
	return &event{cancelFunc: cancelFunc}, nil
}

// Scheme returns the scheme supported by this resolver.  Scheme is defined
// at https://github.com/grpc/grpc/blob/master/doc/naming.md.  The returned
// string should not contain uppercase characters, as they will not match
// the parsed target's scheme as defined in RFC 3986.
func (builder *endpointBuilder) Scheme() string {
	return scheme
}
