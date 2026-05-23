package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	clusterservice "github.com/envoyproxy/go-control-plane/envoy/service/cluster/v3"
	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	endpointservice "github.com/envoyproxy/go-control-plane/envoy/service/endpoint/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/v3"
)

const (
	grpcPort    = 18000
	httpPort    = 8081
	nodeID      = "arbitrage-fleet-node"
	clusterName = "arbitrage_spot_cluster"
)

var version int64

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	snapshotCache := cache.NewSnapshotCache(true, cache.IDHash{}, nil)

	grpcServer := grpc.NewServer(grpc.MaxConcurrentStreams(1000))
	xdsServer := server.NewServer(ctx, snapshotCache, nil)

	discoverygrpc.RegisterAggregatedDiscoveryServiceServer(grpcServer, xdsServer)
	endpointservice.RegisterEndpointDiscoveryServiceServer(grpcServer, xdsServer)
	clusterservice.RegisterClusterDiscoveryServiceServer(grpcServer, xdsServer)

	seedSnapshot(ctx, snapshotCache)

	mux := http.NewServeMux()
	mux.HandleFunc("/update-eds", func(w http.ResponseWriter, r *http.Request) {
		ip := r.URL.Query().Get("ip")
		if ip == "" {
			http.Error(w, "Missing 'ip' parameter", http.StatusBadRequest)
			return
		}

		v := atomic.AddInt64(&version, 1)
		versionStr := fmt.Sprintf("v%d", v)

		edsAssignment := &endpoint.ClusterLoadAssignment{
			ClusterName: clusterName,
			Endpoints: []*endpoint.LocalityLbEndpoints{{
				LbEndpoints: []*endpoint.LbEndpoint{{
					HostIdentifier: &endpoint.LbEndpoint_Endpoint{
						Endpoint: &endpoint.Endpoint{
							Address: &core.Address{
								Address: &core.Address_SocketAddress{
									SocketAddress: &core.SocketAddress{
										Address:       ip,
										PortSpecifier: &core.SocketAddress_PortValue{PortValue: 8080},
									},
								},
							},
						},
					},
					LoadBalancingWeight: &core.UInt32Value{Value: 100},
					HealthStatus:        core.HealthStatus_UNHEALTHY,
				}},
			}},
		}

		snap, err := cache.NewSnapshot(versionStr, map[types.ResponseType]types.Resource{
			cache.EndpointResponse: edsAssignment,
		})
		if err != nil {
			log.Printf("Snapshot generation failed: %v", err)
			return
		}

		if err := snapshotCache.SetSnapshot(ctx, nodeID, snap); err != nil {
			log.Printf("Failed to set snapshot: %v", err)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "EDS Cache mutated to version %s. Delta xDS dispatched.", versionStr)
	})

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", httpPort),
		Handler: mux,
	}
	go func() {
		log.Printf("AI Orchestrator API listening on port %d", httpPort)
		if err := httpServer.ListenAndServe(); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	log.Printf("xDS Control Plane active on port %d", grpcPort)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func seedSnapshot(ctx context.Context, snapshotCache cache.SnapshotCache) {
	edsAssignment := &endpoint.ClusterLoadAssignment{
		ClusterName: clusterName,
		Endpoints: []*endpoint.LocalityLbEndpoints{{
			LbEndpoints: []*endpoint.LbEndpoint{{
				HostIdentifier: &endpoint.LbEndpoint_Endpoint{
					Endpoint: &endpoint.Endpoint{
						Address: &core.Address{
							Address: &core.Address_SocketAddress{
								SocketAddress: &core.SocketAddress{
									Address:       "10.0.0.1",
									PortSpecifier: &core.SocketAddress_PortValue{PortValue: 8080},
								},
							},
						},
					},
				},
				LoadBalancingWeight: &core.UInt32Value{Value: 100},
			}},
		}},
	}

	cdsCluster := &cluster.Cluster{
		Name:                 clusterName,
		ConnectTimeout:       durationpb.New(250 * time.Millisecond),
		ClusterDiscoveryType: &cluster.Cluster_Type{Type: cluster.Cluster_EDS},
		EdsClusterConfig: &cluster.Cluster_EdsClusterConfig{
			EdsConfig: &core.ConfigSource{
				ConfigSourceSpecifier: &core.ConfigSource_Ads{
					Ads: &core.AggregatedConfigSource{},
				},
				ResourceApiVersion: core.ApiVersion_V3,
			},
		},
		LbPolicy: cluster.Cluster_LEAST_REQUEST,
		CommonLbConfig: &cluster.Cluster_CommonLbConfig{
			HealthyPanicThreshold: &core.Percent{Value: 10.0},
		},
	}

	snap, err := cache.NewSnapshot("v0", map[types.ResponseType]types.Resource{
		cache.ClusterResponse:  cdsCluster,
		cache.EndpointResponse: edsAssignment,
	})
	if err != nil {
		log.Fatalf("Seed snapshot failed: %v", err)
	}

	if err := snapshotCache.SetSnapshot(ctx, nodeID, snap); err != nil {
		log.Fatalf("Failed to set seed snapshot: %v", err)
	}
	log.Println("Seed snapshot loaded with initial EDS/CDS config")
}
