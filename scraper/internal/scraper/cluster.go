package scraper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/memberlist"
)

type ClusterConfig struct {
	Name             string   `yaml:"name"`
	NodeName         string   `yaml:"node_name"`
	BindAddress      string   `yaml:"bind_address"`
	AdvertiseAddress string   `yaml:"advertise_address"`
	Join             []string `yaml:"join"`
	RetryInterval    Duration `yaml:"retry_interval"`
}

func (c *ClusterConfig) validate() error {
	if c.Name == "" {
		c.Name = "azure-metrics-scraper"
	}
	if len(c.Name) > 128 || strings.TrimSpace(c.Name) != c.Name {
		return fmt.Errorf("cluster.name must be nonblank and at most 128 bytes")
	}
	if c.BindAddress == "" {
		c.BindAddress = "0.0.0.0:7946"
	}
	if _, _, err := clusterAddress(c.BindAddress, true); err != nil {
		return fmt.Errorf("cluster.bind_address: %w", err)
	}
	if c.AdvertiseAddress != "" {
		if _, _, err := clusterAddress(c.AdvertiseAddress, false); err != nil {
			return fmt.Errorf("cluster.advertise_address: %w", err)
		}
	}
	if c.NodeName != "" && (strings.TrimSpace(c.NodeName) != c.NodeName || len(c.NodeName) > 255) {
		return fmt.Errorf("cluster.node_name must be nonblank and at most 255 bytes")
	}
	for _, seed := range c.Join {
		host, port, err := net.SplitHostPort(seed)
		number, portErr := strconv.Atoi(port)
		if err != nil || portErr != nil || host == "" || strings.ContainsAny(host, " /\\@") || number < 1 || number > 65535 {
			return fmt.Errorf("cluster.join requires host:port seeds, got %q", seed)
		}
	}
	if c.RetryInterval == 0 {
		c.RetryInterval = Duration(5 * time.Second)
	}
	if c.RetryInterval < 0 {
		return fmt.Errorf("cluster.retry_interval must be positive")
	}
	return nil
}

func clusterAddress(address string, bind bool) (string, int, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, fmt.Errorf("expected IP:port: %w", err)
	}
	number, err := strconv.Atoi(port)
	ip := net.ParseIP(host)
	if err != nil || number < 1 || number > 65535 || ip == nil ||
		(!bind && (ip.IsUnspecified() || ip.IsMulticast())) {
		return "", 0, fmt.Errorf("invalid IP:port %q", address)
	}
	return host, number, nil
}

func (c *Config) applyClusterEnvironment() error {
	for _, field := range []struct {
		name string
		set  func(string)
	}{
		{"SCRAPER_CLUSTER_NODE_NAME", func(v string) { c.Cluster.NodeName = v }},
		{"SCRAPER_CLUSTER_BIND_ADDRESS", func(v string) { c.Cluster.BindAddress = v }},
		{"SCRAPER_CLUSTER_ADVERTISE_ADDRESS", func(v string) { c.Cluster.AdvertiseAddress = v }},
		{"SCRAPER_CLUSTER_JOIN", func(v string) { c.Cluster.Join = strings.Split(v, ",") }},
	} {
		if value, exists := os.LookupEnv(field.name); exists {
			if c.Cluster == nil || strings.TrimSpace(value) == "" {
				return fmt.Errorf("%s requires a cluster configuration and a nonempty value", field.name)
			}
			field.set(value)
		}
	}
	if c.Cluster != nil {
		return c.Cluster.validate()
	}
	return nil
}

type Cluster struct {
	list      *memberlist.Memberlist
	logger    *slog.Logger
	config    ClusterConfig
	events    chan struct{}
	ready     atomic.Bool
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

func NewCluster(ctx context.Context, cfg *Config, logger *slog.Logger) (*Cluster, error) {
	if cfg.Cluster == nil {
		return nil, fmt.Errorf("cluster configuration is required")
	}
	settings := *cfg.Cluster
	if err := settings.validate(); err != nil {
		return nil, err
	}
	if settings.NodeName == "" {
		host, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("get cluster hostname: %w", err)
		}
		settings.NodeName = host + "-" + uuid.NewString()
	}
	c := &Cluster{logger: logger, config: settings, events: make(chan struct{}, 1)}
	membership := memberlist.DefaultLANConfig()
	membership.Name, membership.Label = settings.NodeName, settings.Name
	membership.BindAddr, membership.BindPort, _ = clusterAddress(settings.BindAddress, true)
	membership.AdvertisePort = membership.BindPort
	if settings.AdvertiseAddress != "" {
		membership.AdvertiseAddr, membership.AdvertisePort, _ = clusterAddress(settings.AdvertiseAddress, false)
	}
	membership.TCPTimeout = 2 * time.Second
	membership.Logger = slog.NewLogLogger(logger.Handler(), slog.LevelInfo)
	membership.Events = c
	if key, exists := os.LookupEnv("SCRAPER_CLUSTER_SECRET_KEY"); exists {
		decoded, err := base64.StdEncoding.DecodeString(key)
		if err != nil || (len(decoded) != 16 && len(decoded) != 24 && len(decoded) != 32) {
			return nil, fmt.Errorf("SCRAPER_CLUSTER_SECRET_KEY must be base64 encoding of 16, 24 or 32 bytes")
		}
		membership.SecretKey = decoded
	} else {
		logger.Warn("gossip encryption is disabled; use a trusted private network")
	}
	list, err := memberlist.Create(membership)
	if err != nil {
		return nil, fmt.Errorf("create memberlist: %w", err)
	}
	c.list = list
	ctx, c.cancel = context.WithCancel(ctx)
	if len(settings.Join) == 0 {
		logger.Warn("cluster has no join seeds; bootstrapping a new cluster", "node", settings.NodeName)
		c.ready.Store(true)
	} else {
		c.wg.Go(func() { c.joinLoop(ctx) })
	}
	logger.Info("memberlist started", "node", c.Name(), "address", list.LocalNode().Address(), "cluster", settings.Name)
	return c, nil
}

func (c *Cluster) joinLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(c.config.RetryInterval))
	defer ticker.Stop()
	for ctx.Err() == nil {
		joined, err := c.list.Join(c.config.Join)
		if err != nil && ctx.Err() == nil {
			c.logger.Warn("cluster join failed or partially completed; retrying", "error", err)
		}
		if joined > 0 && !c.ready.Swap(true) {
			c.notify()
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *Cluster) Name() string             { return c.list.LocalNode().Name }
func (c *Cluster) Ready() bool              { return c.ready.Load() }
func (c *Cluster) Changes() <-chan struct{} { return c.events }

func (c *Cluster) Members() []string {
	nodes := c.list.Members()
	names := make([]string, 0, len(nodes))
	for _, node := range nodes {
		names = append(names, node.Name)
	}
	slices.Sort(names)
	return names
}

func (c *Cluster) notify() {
	select {
	case c.events <- struct{}{}:
	default:
	}
}

func (c *Cluster) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		c.wg.Wait()
		c.ready.Store(false)
		c.notify()
		leaveErr := c.list.Leave(2 * time.Second)
		shutdownErr := c.list.Shutdown()
		c.closeErr = errors.Join(leaveErr, shutdownErr)
	})
	return c.closeErr
}

func taskOwner(id string, members []string) string {
	owner := ""
	var highest [sha256.Size]byte
	for _, member := range members {
		data, _ := json.Marshal([]string{id, member})
		score := sha256.Sum256(data)
		comparison := bytes.Compare(score[:], highest[:])
		if owner == "" || comparison > 0 || (comparison == 0 && member > owner) {
			owner, highest = member, score
		}
	}
	return owner
}

func (c *Cluster) NotifyJoin(*memberlist.Node)   { c.notify() }
func (c *Cluster) NotifyLeave(*memberlist.Node)  { c.notify() }
func (c *Cluster) NotifyUpdate(*memberlist.Node) { c.notify() }
