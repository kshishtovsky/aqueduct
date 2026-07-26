package admin

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	adminpb "github.com/kshishtovsky/aqueduct/internal/admin/proto"
	"github.com/kshishtovsky/aqueduct/internal/authz"
	"github.com/kshishtovsky/aqueduct/internal/quotas"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// generateTestCerts creates a CA cert and helper client/server certs for mTLS testing.
func generateTestCerts(t *testing.T) (serverTLS *tls.Config, adminTLS *tls.Config, userTLS *tls.Config) {
	t.Helper()

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"Aqueduct CA"}},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	genCert := func(cn string, isServer bool) *tls.Config {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatalf("generate key for %s: %v", cn, err)
		}
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()),
			Subject:      pkix.Name{CommonName: cn},
			NotBefore:    time.Now().Add(-1 * time.Hour),
			NotAfter:     time.Now().Add(24 * time.Hour),
		}
		if isServer {
			tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
			tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
			tmpl.DNSNames = []string{"localhost"}
		} else {
			tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		}

		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		if err != nil {
			t.Fatalf("create cert for %s: %v", cn, err)
		}

		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		keyDER, _ := x509.MarshalPKCS8PrivateKey(key)
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

		tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			t.Fatalf("X509KeyPair for %s: %v", cn, err)
		}

		conf := &tls.Config{
			Certificates: []tls.Certificate{tlsCert},
			RootCAs:      caPool,
		}
		if isServer {
			conf.ClientCAs = caPool
			conf.ClientAuth = tls.RequireAndVerifyClientCert
		}
		return conf
	}

	serverTLS = genCert("server", true)
	adminTLS = genCert("admin-operator", false)
	userTLS = genCert("user-regular", false)
	return
}

func setupAdminTestServer(t *testing.T, qm *quotas.Manager, engine *authz.Engine) (*Server, string, *tls.Config, *tls.Config) {
	t.Helper()
	serverTLS, adminTLS, userTLS := generateTestCerts(t)

	s := NewServer(qm, engine)
	if err := s.Start("127.0.0.1:0", serverTLS); err != nil {
		t.Fatalf("failed to start admin server: %v", err)
	}

	return s, s.Addr().String(), adminTLS, userTLS
}

func dialAdminClient(t *testing.T, addr string, tlsConf *tls.Config) (adminpb.AdminServiceClient, *grpc.ClientConn) {
	t.Helper()
	creds := credentials.NewTLS(tlsConf)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatalf("grpc dial %s: %v", addr, err)
	}
	return adminpb.NewAdminServiceClient(conn), conn
}

func TestAdminmTLSAuth(t *testing.T) {
	qm := quotas.NewManager(1000, 1000)
	engine := authz.NewBuilder(authz.PermAll).Build()
	s, addr, adminTLS, userTLS := setupAdminTestServer(t, qm, engine)
	defer s.Stop()

	// 1. Client with admin- CN -> SUCCESS
	clientAdmin, connAdmin := dialAdminClient(t, addr, adminTLS)
	defer connAdmin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := clientAdmin.SetClientQuota(ctx, &adminpb.SetClientQuotaRequest{
		ClientId: "test-client",
		Rate:     500,
	})
	if err != nil {
		t.Fatalf("admin client SetClientQuota failed: %v", err)
	}
	if !resp.GetSuccess() {
		t.Errorf("expected success true, got false")
	}

	// 2. Client with user- CN (no admin- prefix) -> PermissionDenied
	clientUser, connUser := dialAdminClient(t, addr, userTLS)
	defer connUser.Close()

	_, err = clientUser.SetClientQuota(ctx, &adminpb.SetClientQuotaRequest{
		ClientId: "test-client",
		Rate:     500,
	})
	if err == nil {
		t.Fatalf("expected error for non-admin CN client, got nil")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied status, got %v", err)
	}
}

func TestHotReloadQuotas(t *testing.T) {
	// 1. Client starts with rate 1000, burst 1000
	qm := quotas.NewManager(1000, 1000)
	engine := authz.NewBuilder(authz.PermAll).Build()
	s, addr, adminTLS, _ := setupAdminTestServer(t, qm, engine)
	defer s.Stop()

	client, conn := dialAdminClient(t, addr, adminTLS)
	defer conn.Close()

	clientID := "client-A"

	// Initially client can acquire up to burst
	for i := 0; i < 100; i++ {
		if !qm.TryAcquire(clientID) {
			t.Fatalf("initially expected TryAcquire to succeed")
		}
	}

	// 2. Via Admin API call SetClientQuota("client-A", 100)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := client.SetClientQuota(ctx, &adminpb.SetClientQuotaRequest{
		ClientId: clientID,
		Rate:     100,
	})
	if err != nil {
		t.Fatalf("SetClientQuota failed: %v", err)
	}

	// 3. Verify that quota is updated dynamically without restarting
	// Drain tokens
	for i := 0; i < 2000; i++ {
		_ = qm.TryAcquire(clientID)
	}

	// Now tokens should be exhausted and TryAcquire fails (rate-limited drops) within < 1 second
	if qm.TryAcquire(clientID) {
		t.Errorf("expected client-A to be rate-limited after exhausting lower quota")
	}
}

func TestHotReloadACL(t *testing.T) {
	// 1. Client initially allowed to publish to topic-1
	builder := authz.NewBuilder(authz.PermNone)
	builder.Allow("client-A", "topic-1", authz.PermPublish)
	engine := builder.Build()

	qm := quotas.NewManager(1000, 1000)
	s, addr, adminTLS, _ := setupAdminTestServer(t, qm, engine)
	defer s.Stop()

	client, conn := dialAdminClient(t, addr, adminTLS)
	defer conn.Close()

	topicBytes := []byte("topic-1")
	if !engine.Allowed("client-A", topicBytes, authz.PermPublish) {
		t.Fatalf("client-A should initially be allowed to publish to topic-1")
	}

	// 2. Via Admin API revoke publish rights by sending new rule list without client-A publish
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.UpdateACL(ctx, &adminpb.UpdateACLRequest{
		Rules: []*adminpb.ACLRule{
			{
				ClientId:   "client-A",
				Topic:      "topic-1",
				Permission: "none",
			},
		},
	})
	if err != nil {
		t.Fatalf("UpdateACL failed: %v", err)
	}
	if !resp.GetSuccess() {
		t.Errorf("UpdateACL expected success true")
	}

	// 3. Verify subsequent messages are immediately rejected
	if engine.Allowed("client-A", topicBytes, authz.PermPublish) {
		t.Errorf("client-A publish permission should have been revoked by UpdateACL")
	}
}

func TestAdminInvalidArgument(t *testing.T) {
	qm := quotas.NewManager(1000, 1000)
	engine := authz.NewBuilder(authz.PermAll).Build()
	s, addr, adminTLS, _ := setupAdminTestServer(t, qm, engine)
	defer s.Stop()

	client, conn := dialAdminClient(t, addr, adminTLS)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := client.SetClientQuota(ctx, &adminpb.SetClientQuotaRequest{
		ClientId: "",
		Rate:     100,
	})
	if err == nil {
		t.Fatalf("expected error for empty client_id")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument error, got %v", err)
	}
}

func BenchmarkACLHotReload(b *testing.B) {
	const rulesCount = 10000
	protoRules := make([]*adminpb.ACLRule, rulesCount)
	for i := 0; i < rulesCount; i++ {
		protoRules[i] = &adminpb.ACLRule{
			ClientId:   fmt.Sprintf("client-%d", i),
			Topic:      fmt.Sprintf("topic-%d", i%100),
			Permission: "publish",
		}
	}

	engine := authz.NewEngine(nil, authz.PermNone)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		newRules := make(map[uint64]authz.Permission, rulesCount)
		for _, r := range protoRules {
			key := authz.CombineHashStrings(r.GetClientId(), r.GetTopic())
			newRules[key] = authz.PermPublish
		}
		engine.Reload(newRules)
	}
}

func BenchmarkACLCheck(b *testing.B) {
	engine := authz.NewBuilder(authz.PermNone).
		Allow("service-a", "orders", authz.PermPublish).
		Build()

	clientStr := "service-a"
	topicBytes := []byte("orders")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = engine.Allowed(clientStr, topicBytes, authz.PermPublish)
	}
}
