package grpc_kubernetes_resolver

import (
	"context"
	"google.golang.org/grpc/grpclog"
	grpc_rosover "google.golang.org/grpc/resolver"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"sort"
	"strconv"
)

func init() {
	grpc_rosover.Register(&endpointBuilder{})

}

type event struct {
	cancelFunc context.CancelFunc
}

// ResolveNow will be called by gRPC to try to resolve the target name
// again. It's just a hint, resolver can ignore this if it's not necessary.
//
// It could be called multiple times concurrently.
func (e *event) ResolveNow(grpc_rosover.ResolveNowOptions) {}

// Close closes the resolver.
func (e *event) Close() {
	e.cancelFunc()
}

func endpointNetworkWatch(ctx context.Context, namespace, name string, pipe chan<- []string) {
	cfg, err := clientcmd.BuildConfigFromFlags("", "")
	if err != nil {
		grpclog.Errorf("[grpc-kubernetes-resolver] Could not build k8s config,err='%s'", err.Error())
		return
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		grpclog.Errorf("[grpc-kubernetes-resolver] Could not build k8s client set,err='%s'", err.Error())
		return
	}

	lw := cache.NewListWatchFromClient(clientset.CoreV1().RESTClient(), "endpoints", namespace, fields.OneTermEqualSelector("metadata.name", name))
	eventChan, err := lw.Watch(metav1.ListOptions{})
	if err != nil {
		grpclog.Errorf("[grpc-kubernetes-resolver] Could not watch  endpoints,err='%s'", err.Error())
		return
	}

	for {
		select {
		case event := <-eventChan.ResultChan():
			switch event.Type {
			case watch.Added:
				fallthrough
			case watch.Modified:
				fallthrough
			case watch.Bookmark:
				obj := event.Object
				ep := obj.(*v1.Endpoints)
				pipe <- epAsAddress(ep)
			case watch.Deleted:
				obj := event.Object
				ep := obj.(*v1.Endpoints)
				grpclog.Infof("[grpc-kubernetes-resolver] endpoint '%s/%s' has update for '%s'", ep.Namespace, ep.Name, "[]")
				pipe <- make([]string, 0)
			case watch.Error:
				grpclog.Error("[grpc-kubernetes-resolver] Watch Endpoints appear error events")
				continue
			}

		case <-ctx.Done():
			eventChan.Stop()
			return
		}
	}

}

func populateTarget(ctx context.Context, cc grpc_rosover.ClientConn, pipe <-chan []string) {
	for {
		select {
		case address := <-pipe:
			var state grpc_rosover.State
			state.Addresses = make([]grpc_rosover.Address, 0, len(address))
			for _, addr := range address {
				addr := addr
				state.Addresses = append(state.Addresses, grpc_rosover.Address{Addr: addr})
			}
			if err := cc.UpdateState(state); err != nil {
				grpclog.Info("[grpc-kubernetes-resolver] Could not update client Connection,err='%s'", err.Error())
				continue
			}
		case <-ctx.Done():
			grpclog.Info("[grpc-kubernetes-resolver] Watch has bean finished")
			return
		}
	}
}

func epAsAddress(ep *v1.Endpoints) []string {
	addrs := make([]string, 0)
	for _, epsub := range ep.Subsets {
		for _, epsubAddr := range epsub.Addresses {
			epsubAddr := epsubAddr
			for _, epsubPort := range epsub.Ports {
				epsubPort := epsubPort
				addrs = append(addrs, epsubAddr.IP+":"+strconv.Itoa(int(epsubPort.Port)))
			}
		}
	}
	// filter repeted
	addrMap := make(map[string]struct{}, len(addrs))
	for _, addr := range addrs {
		addr := addr
		addrMap[addr] = struct{}{}
	}
	addrs = make([]string, 0, len(addrMap))
	for addr := range addrMap {
		addrs = append(addrs, addr)
	}
	sort.Strings(addrs)
	grpclog.Infof("[grpc-kubernetes-resolver] endpoint '%s/%s' has update for '%s'", ep.Namespace, ep.Name, addrs)
	return addrs
}
