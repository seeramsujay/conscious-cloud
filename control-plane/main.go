package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	accesslog "github.com/envoyproxy/go-control-plane/envoy/config/accesslog/v3"
	fileaccesslog "github.com/envoyproxy/go-control-plane/envoy/extensions/access_loggers/file/v3"
	hcm "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	router "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	clusterservice "github.com/envoyproxy/go-control-plane/envoy/service/cluster/v3"
	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	endpointservice "github.com/envoyproxy/go-control-plane/envoy/service/endpoint/v3"
	listenerservice "github.com/envoyproxy/go-control-plane/envoy/service/listener/v3"
	routeservice "github.com/envoyproxy/go-control-plane/envoy/service/route/v3"
	envoytype "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/v3"
)

const (
	grpcPort    = 18000
	httpPort    = 8082
	nodeID      = "arbitrage-fleet-node"
	clusterName = "arbitrage_spot_cluster"
)

var (
	version     int64
	cdsCluster  *cluster.Cluster
	ldsListener *listener.Listener
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	snapshotCache := cache.NewSnapshotCache(true, cache.IDHash{}, nil)

	grpcServer := grpc.NewServer(grpc.MaxConcurrentStreams(1000))
	xdsServer := server.NewServer(ctx, snapshotCache, nil)

	discoverygrpc.RegisterAggregatedDiscoveryServiceServer(grpcServer, xdsServer)
	endpointservice.RegisterEndpointDiscoveryServiceServer(grpcServer, xdsServer)
	clusterservice.RegisterClusterDiscoveryServiceServer(grpcServer, xdsServer)
	listenerservice.RegisterListenerDiscoveryServiceServer(grpcServer, xdsServer)
	routeservice.RegisterRouteDiscoveryServiceServer(grpcServer, xdsServer)

	seedSnapshot(ctx, snapshotCache)

	mux := http.NewServeMux()
	mux.HandleFunc("/update-eds", func(w http.ResponseWriter, r *http.Request) {
		ipsParam := r.URL.Query().Get("ips")
		if ipsParam == "" {
			// Backwards compatibility with the single-IP query param
			ip := r.URL.Query().Get("ip")
			if ip != "" {
				ipsParam = ip
			} else {
				http.Error(w, "Missing 'ips' or 'ip' parameter", http.StatusBadRequest)
				return
			}
		}

		weightsParam := r.URL.Query().Get("weights")
		healthsParam := r.URL.Query().Get("healths")

		ips := strings.Split(ipsParam, ",")
		var weights []string
		if weightsParam != "" {
			weights = strings.Split(weightsParam, ",")
		}
		var healths []string
		if healthsParam != "" {
			healths = strings.Split(healthsParam, ",")
		}

		var lbEndpoints []*endpoint.LbEndpoint
		for i, ip := range ips {
			weight := uint32(100)
			if i < len(weights) {
				var wVal uint32
				if _, err := fmt.Sscanf(weights[i], "%d", &wVal); err == nil {
					weight = wVal
				}
			}
			if weight == 0 {
				weight = 1
			}

			// Default health is UNHEALTHY as per Blackhole Mitigation spec
			health := core.HealthStatus_UNHEALTHY
			if i < len(healths) {
				switch strings.ToLower(healths[i]) {
				case "healthy":
					health = core.HealthStatus_HEALTHY
				case "unhealthy":
					health = core.HealthStatus_UNHEALTHY
				case "draining":
					health = core.HealthStatus_DRAINING
				}
			}

			lbEndpoints = append(lbEndpoints, &endpoint.LbEndpoint{
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
				LoadBalancingWeight: wrapperspb.UInt32(weight),
				HealthStatus:        health,
			})
		}

		v := atomic.AddInt64(&version, 1)
		versionStr := fmt.Sprintf("v%d", v)

		edsAssignment := &endpoint.ClusterLoadAssignment{
			ClusterName: clusterName,
			Endpoints: []*endpoint.LocalityLbEndpoints{{
				LbEndpoints: lbEndpoints,
			}},
		}

		snap, err := cache.NewSnapshot(versionStr, map[resource.Type][]types.Resource{
			resource.ClusterType:  {cdsCluster},
			resource.EndpointType: {edsAssignment},
			resource.ListenerType: {ldsListener},
		})
		if err != nil {
			log.Printf("Snapshot generation failed: %v", err)
			http.Error(w, fmt.Sprintf("Snapshot failed: %v", err), http.StatusInternalServerError)
			return
		}

		if err := snapshotCache.SetSnapshot(ctx, nodeID, snap); err != nil {
			log.Printf("Failed to set snapshot: %v", err)
			http.Error(w, fmt.Sprintf("Failed to set snapshot: %v", err), http.StatusInternalServerError)
			return
		}

		log.Printf("EDS Cache mutated to version %s. Endpoints: %s", versionStr, ipsParam)
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

func makeIngressListener(clusterName string) (*listener.Listener, error) {
	routeConfig := &route.RouteConfiguration{
		Name: "route_config",
		VirtualHosts: []*route.VirtualHost{{
			Name:    "all_hosts",
			Domains: []string{"*"},
			Routes: []*route.Route{{
				Match: &route.RouteMatch{
					PathSpecifier: &route.RouteMatch_Prefix{Prefix: "/"},
				},
				Action: &route.Route_Route{
					Route: &route.RouteAction{
						ClusterSpecifier: &route.RouteAction_Cluster{
							Cluster: clusterName,
						},
						RetryPolicy: &route.RetryPolicy{
							RetryOn:    "connect-failure,refused-stream,5xx",
							NumRetries: wrapperspb.UInt32(3),
							RetryBackOff: &route.RetryPolicy_RetryBackOff{
								BaseInterval: durationpb.New(25 * time.Millisecond),
							},
						},
					},
				},
			}},
		}},
	}

	routerConfig, err := anypb.New(&router.Router{})
	if err != nil {
		return nil, err
	}

	accessLogConfig, err := anypb.New(&fileaccesslog.FileAccessLog{
		Path: "/dev/stdout",
	})
	if err != nil {
		return nil, err
	}

	manager := &hcm.HttpConnectionManager{
		CodecType:  hcm.HttpConnectionManager_AUTO,
		StatPrefix: "ingress_http",
		RouteSpecifier: &hcm.HttpConnectionManager_RouteConfig{
			RouteConfig: routeConfig,
		},
		HttpFilters: []*hcm.HttpFilter{{
			Name: "envoy.filters.http.router",
			ConfigType: &hcm.HttpFilter_TypedConfig{
				TypedConfig: routerConfig,
			},
		}},
		AccessLog: []*accesslog.AccessLog{{
			Name: "envoy.access_loggers.file",
			ConfigType: &accesslog.AccessLog_TypedConfig{
				TypedConfig: accessLogConfig,
			},
		}},
		CommonHttpProtocolOptions: &core.HttpProtocolOptions{
			MaxConnectionDuration: durationpb.New(5 * time.Second),
		},
		DrainTimeout:        durationpb.New(2 * time.Second),
		DelayedCloseTimeout: durationpb.New(1 * time.Second),
	}

	pbst, err := anypb.New(manager)
	if err != nil {
		return nil, err
	}

	return &listener.Listener{
		Name: "ingress_listener",
		Address: &core.Address{
			Address: &core.Address_SocketAddress{
				SocketAddress: &core.SocketAddress{
					Protocol: core.SocketAddress_TCP,
					Address:  "0.0.0.0",
					PortSpecifier: &core.SocketAddress_PortValue{
						PortValue: 10000,
					},
				},
			},
		},
		FilterChains: []*listener.FilterChain{{
			Filters: []*listener.Filter{{
				Name: "envoy.filters.network.http_connection_manager",
				ConfigType: &listener.Filter_TypedConfig{
					TypedConfig: pbst,
				},
			}},
		}},
	}, nil
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
				LoadBalancingWeight: wrapperspb.UInt32(100),
				// Default seed endpoint is healthy so it's ready initially
				HealthStatus: core.HealthStatus_HEALTHY,
			}},
		}},
	}

	cdsCluster = &cluster.Cluster{
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
			HealthyPanicThreshold: &envoytype.Percent{Value: 10.0},
		},
		HealthChecks: []*core.HealthCheck{{
			Timeout:            durationpb.New(2 * time.Second),
			Interval:           durationpb.New(5 * time.Second),
			UnhealthyThreshold: wrapperspb.UInt32(2),
			HealthyThreshold:   wrapperspb.UInt32(2),
			HealthChecker: &core.HealthCheck_HttpHealthCheck_{
				HttpHealthCheck: &core.HealthCheck_HttpHealthCheck{
					Path: "/healthz",
				},
			},
		}},
		OutlierDetection: &cluster.OutlierDetection{
			Consecutive_5Xx:    wrapperspb.UInt32(3),
			Interval:           durationpb.New(10 * time.Second),
			BaseEjectionTime:   durationpb.New(30 * time.Second),
			MaxEjectionPercent: wrapperspb.UInt32(50),
		},
	}

	var err error
	ldsListener, err = makeIngressListener(clusterName)
	if err != nil {
		log.Fatalf("Failed to create ingress listener: %v", err)
	}

	snap, err := cache.NewSnapshot("v0", map[resource.Type][]types.Resource{
		resource.ClusterType:  {cdsCluster},
		resource.EndpointType: {edsAssignment},
		resource.ListenerType: {ldsListener},
	})
	if err != nil {
		log.Fatalf("Seed snapshot failed: %v", err)
	}

	if err := snapshotCache.SetSnapshot(ctx, nodeID, snap); err != nil {
		log.Fatalf("Failed to set seed snapshot: %v", err)
	}
	log.Println("Seed snapshot loaded with initial LDS/EDS/CDS config")
}
