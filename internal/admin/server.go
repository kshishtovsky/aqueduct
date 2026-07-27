package admin

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"

	adminpb "github.com/kshishtovsky/aqueduct/internal/admin/proto"
	"github.com/kshishtovsky/aqueduct/internal/authz"
	"github.com/kshishtovsky/aqueduct/internal/metrics"
	"github.com/kshishtovsky/aqueduct/internal/quotas"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Server implements the gRPC Admin API for dynamic configuration hot-reloading.
type Server struct {
	adminpb.UnimplementedAdminServiceServer
	quotaManager *quotas.Manager
	authzEngine  *authz.Engine
	logger       *slog.Logger
	listener     net.Listener
	grpcServer   *grpc.Server
}

// Option configures the admin Server.
type Option func(*Server)

// WithLogger sets the structured logger for the admin server.
func WithLogger(logger *slog.Logger) Option {
	return func(s *Server) {
		if logger != nil {
			s.logger = logger
		}
	}
}

// NewServer initializes a new gRPC admin Server.
func NewServer(quotaManager *quotas.Manager, authzEngine *authz.Engine, opts ...Option) *Server {
	s := &Server{
		quotaManager: quotaManager,
		authzEngine:  authzEngine,
		logger:       slog.Default(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Start launches the gRPC admin server on the given TCP address with mTLS configuration.
func (s *Server) Start(addr string, tlsConfig *tls.Config) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("admin server listen %s: %w", addr, err)
	}
	s.listener = lis

	var grpcOpts []grpc.ServerOption
	if tlsConfig != nil {
		creds := credentials.NewTLS(tlsConfig)
		grpcOpts = append(grpcOpts, grpc.Creds(creds))
	}
	grpcOpts = append(grpcOpts, grpc.UnaryInterceptor(adminAuthInterceptor(s.logger)))

	s.grpcServer = grpc.NewServer(grpcOpts...)
	adminpb.RegisterAdminServiceServer(s.grpcServer, s)

	go func() {
		if err := s.grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			s.logger.Error("admin gRPC server serve error", "err", err)
		}
	}()

	s.logger.Info("admin gRPC server started", "addr", lis.Addr().String())
	return nil
}

// Addr returns the net.Addr of the running admin listener (useful for tests on port :0).
func (s *Server) Addr() net.Addr {
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

// Stop gracefully shuts down the gRPC admin server.
func (s *Server) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if s.listener != nil {
		_ = s.listener.Close()
	}
}

// SetClientQuota dynamically updates the rate limit token bucket refill rate for a client.
func (s *Server) SetClientQuota(ctx context.Context, req *adminpb.SetClientQuotaRequest) (*adminpb.SetClientQuotaResponse, error) {
	metrics.AdminRequestsTotal.WithLabelValues("SetClientQuota").Inc()

	callerCN := s.extractCallerCN(ctx)
	s.logger.Info("admin API: SetClientQuota",
		"caller", callerCN,
		"client_id", req.GetClientId(),
		"rate", req.GetRate(),
	)

	if req.GetClientId() == "" {
		return nil, status.Error(codes.InvalidArgument, "client_id cannot be empty")
	}

	if s.quotaManager != nil {
		s.quotaManager.SetRate(req.GetClientId(), int(req.GetRate()), 0)
	}

	return &adminpb.SetClientQuotaResponse{Success: true}, nil
}

// UpdateACL dynamically rebuilds ACL rules and updates authz.Engine via RCU (atomic.Pointer).
func (s *Server) UpdateACL(ctx context.Context, req *adminpb.UpdateACLRequest) (*adminpb.UpdateACLResponse, error) {
	metrics.AdminRequestsTotal.WithLabelValues("UpdateACL").Inc()

	callerCN := s.extractCallerCN(ctx)
	s.logger.Info("admin API: UpdateACL",
		"caller", callerCN,
		"rules_count", len(req.GetRules()),
	)

	if s.authzEngine == nil {
		return nil, status.Error(codes.Unavailable, "authorization engine is not enabled")
	}

	newRules := make(map[uint64]authz.Permission, len(req.GetRules()))
	for _, r := range req.GetRules() {
		var perm authz.Permission
		switch strings.ToLower(strings.TrimSpace(r.GetPermission())) {
		case "publish":
			perm = authz.PermPublish
		case "subscribe":
			perm = authz.PermSubscribe
		case "all":
			perm = authz.PermAll
		case "none":
			perm = authz.PermNone
		default:
			perm = authz.PermNone
		}
		key := authz.CombineHashStrings(r.GetClientId(), r.GetTopic())
		newRules[key] |= perm
	}

	// Hot-Reload RCU: swap rules map pointer atomically.
	s.authzEngine.Reload(newRules)

	return &adminpb.UpdateACLResponse{
		Success: true,
		// #nosec G115 -- ACL rules are operator-controlled and bounded by config; the proto schema mandates int32 here.
		RulesCount: int32(len(newRules)),
	}, nil
}

func (s *Server) extractCallerCN(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.AuthInfo != nil {
		if tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo); ok && len(tlsInfo.State.PeerCertificates) > 0 {
			return tlsInfo.State.PeerCertificates[0].Subject.CommonName
		}
	}
	return "unknown"
}

func adminAuthInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		p, ok := peer.FromContext(ctx)
		if !ok || p.AuthInfo == nil {
			return nil, status.Error(codes.Unauthenticated, "missing peer authentication context")
		}
		tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
		if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing client TLS certificate")
		}
		cn := tlsInfo.State.PeerCertificates[0].Subject.CommonName
		if !strings.HasPrefix(cn, "admin-") {
			logger.Warn("admin API access denied: invalid common name", "cn", cn, "method", info.FullMethod)
			return nil, status.Errorf(codes.PermissionDenied, "access denied: client CN %q does not have admin role", cn)
		}
		return handler(ctx, req)
	}
}
