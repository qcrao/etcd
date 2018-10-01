// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdserver

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/coreos/etcd/pkg/netutil"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/coreos/etcd/pkg/types"
)

// ServerConfig holds the configuration of etcd as taken from the command line or discovery.
type ServerConfig struct {
	// etcdserver 名称，对应flag "name“
	Name           string
	// etcd 用于服务发现，无需知道具体etcd节点ip即可访问etcd 服务，对应flag  "discovery"
	DiscoveryURL   string
	// 供服务发现url的代理地址， 对应flag "discovery-proxy"
	DiscoveryProxy string
	// 由ip+port组成，默认DefaultListenClientURLs = "http://localhost:2379";
	// 实际情况使用https schema，用以外部etcd client访问，对应flag "listen-client-urls"
	ClientURLs     types.URLs
	// 由ip+port组成，默认DefaultListenPeerURLs   = "http://localhost:2380";
	// 实际生产环境使用http schema, 供etcd member 通信，对应flag "peer-client-urls"
	PeerURLs       types.URLs
	// 数据目录地址，为全路径，对应flag "data-dir"
	DataDir        string
	// DedicatedWALDir config will make the etcd to write the WAL to the WALDir
	// rather than the dataDir/member/wal.
	// 此字段使etcd将wal日志改写到此路径。
	DedicatedWALDir     string
	// 默认是10000次事件做一次快照:DefaultSnapCount = 100000
	// 可以作为调优参数进行参考，对应flag "snapshot-count",
	SnapCount           uint64
	// 默认是5，这是v2的参数，v3内只有一个db文件，
	// DefaultMaxSnapshots = 5，对应flag "max-snapshots"
	MaxSnapFiles        uint
	// 默认是5，DefaultMaxWALs      = 5，表示最大存储wal文件的个数，
	// 对应flag "max-wals"，保留的文件可以作为etcd-dump-logs工具进行debug使用。
	MaxWALFiles         uint
	// peerUrl 与 etcd name对应的map,由方法cfg.PeerURLsMapAndToken("etcd")生成。
	InitialPeerURLsMap  types.URLsMap
	// etcd 集群token, 对应flang "initial-cluster-token"
	InitialClusterToken string
	// 确定是否为新建集群，对应flag "initial-cluster-state"
	NewCluster          bool
	// 对应flag "force-new-cluster",默认为false,若为true，
	// 在生产环境内，一般用于含v2数据的集群恢复.
	// 效果为以现有数据或者空数据新建一个单节点的etcd集群，
	// 如果存在元数据，则会清掉数据内的元数据信息，并重建只包含该etcd的元数据信息。
	ForceNewCluster     bool
	// member间通信使用的证书信息，若peerURL为https时使用，对应flag "peer-ca-file","peer-cert-file", "peer-key-file"
	PeerTLSInfo         transport.TLSInfo
	// raft node 发送心跳信息的超时时间。 "heartbeat-interval"
	TickMs        uint
	// raft node 发起选举的超时时间，最大为5000ms maxElectionMs = 50000,
	// 对应flag "election-timeout",
	// 选举时间与心跳时间在最佳实践内建议是10倍关系。
	ElectionTicks int

	// InitialElectionTickAdvance is true, then local member fast-forwards
	// election ticks to speed up "initial" leader election trigger. This
	// benefits the case of larger election ticks. For instance, cross
	// datacenter deployment may require longer election timeout of 10-second.
	// If true, local node does not need wait up to 10-second. Instead,
	// forwards its election ticks to 8-second, and have only 2-second left
	// before leader election.
	//
	// Major assumptions are that:
	//  - cluster has no active leader thus advancing ticks enables faster
	//    leader election, or
	//  - cluster already has an established leader, and rejoining follower
	//    is likely to receive heartbeats from the leader after tick advance
	//    and before election timeout.
	//
	// However, when network from leader to rejoining follower is congested,
	// and the follower does not receive leader heartbeat within left election
	// ticks, disruptive election has to happen thus affecting cluster
	// availabilities.
	//
	// Disabling this would slow down initial bootstrap process for cross
	// datacenter deployments. Make your own tradeoffs by configuring
	// --initial-election-tick-advance at the cost of slow initial bootstrap.
	//
	// If single-node, it advances ticks regardless.
	//
	// See https://github.com/coreos/etcd/issues/9333 for more detail.
	InitialElectionTickAdvance bool

	BootstrapTimeout time.Duration

	// 默认为0，单位为小时，主要为了方便用户快速查询，
	// 定时对key进行合并处理，对应flag "auto-compaction-retention",
	// 由方法func NewPeriodic(h int, rg RevGetter, c Compactable) *Periodic确定，
	// 具体compact的实现方法为：
	//func (s *kvServer) Compact(ctx context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error)
	AutoCompactionRetention time.Duration
	AutoCompactionMode      string
	// etcd后端数据文件的大小，默认为2GB，最大为8GB, v3的参数，
	// 对应flag  "quota-backend-bytes" ，具体定义：etcd\etcdserver\quota.go
	QuotaBackendBytes       int64
	MaxTxnOps               uint

	// MaxRequestBytes is the maximum request size to send over raft.
	MaxRequestBytes uint

	StrictReconfigCheck bool

	// ClientCertAuthEnabled is true when cert has been signed by the client CA.
	ClientCertAuthEnabled bool

	AuthToken string

	// InitialCorruptCheck is true to check data corruption on boot
	// before serving any peer/client traffic.
	InitialCorruptCheck bool
	CorruptCheckTime    time.Duration

	Debug bool
}

// VerifyBootstrap sanity-checks the initial config for bootstrap case
// and returns an error for things that should never happen.
func (c *ServerConfig) VerifyBootstrap() error {
	if err := c.hasLocalMember(); err != nil {
		return err
	}
	if err := c.advertiseMatchesCluster(); err != nil {
		return err
	}
	if checkDuplicateURL(c.InitialPeerURLsMap) {
		return fmt.Errorf("initial cluster %s has duplicate url", c.InitialPeerURLsMap)
	}
	if c.InitialPeerURLsMap.String() == "" && c.DiscoveryURL == "" {
		return fmt.Errorf("initial cluster unset and no discovery URL found")
	}
	return nil
}

// VerifyJoinExisting sanity-checks the initial config for join existing cluster
// case and returns an error for things that should never happen.
func (c *ServerConfig) VerifyJoinExisting() error {
	// The member has announced its peer urls to the cluster before starting; no need to
	// set the configuration again.
	if err := c.hasLocalMember(); err != nil {
		return err
	}
	if checkDuplicateURL(c.InitialPeerURLsMap) {
		return fmt.Errorf("initial cluster %s has duplicate url", c.InitialPeerURLsMap)
	}
	if c.DiscoveryURL != "" {
		return fmt.Errorf("discovery URL should not be set when joining existing initial cluster")
	}
	return nil
}

// hasLocalMember checks that the cluster at least contains the local server.
func (c *ServerConfig) hasLocalMember() error {
	if urls := c.InitialPeerURLsMap[c.Name]; urls == nil {
		return fmt.Errorf("couldn't find local name %q in the initial cluster configuration", c.Name)
	}
	return nil
}

// advertiseMatchesCluster confirms peer URLs match those in the cluster peer list.
func (c *ServerConfig) advertiseMatchesCluster() error {
	urls, apurls := c.InitialPeerURLsMap[c.Name], c.PeerURLs.StringSlice()
	urls.Sort()
	sort.Strings(apurls)
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()
	ok, err := netutil.URLStringsEqual(ctx, apurls, urls.StringSlice())
	if ok {
		return nil
	}

	initMap, apMap := make(map[string]struct{}), make(map[string]struct{})
	for _, url := range c.PeerURLs {
		apMap[url.String()] = struct{}{}
	}
	for _, url := range c.InitialPeerURLsMap[c.Name] {
		initMap[url.String()] = struct{}{}
	}

	missing := []string{}
	for url := range initMap {
		if _, ok := apMap[url]; !ok {
			missing = append(missing, url)
		}
	}
	if len(missing) > 0 {
		for i := range missing {
			missing[i] = c.Name + "=" + missing[i]
		}
		mstr := strings.Join(missing, ",")
		apStr := strings.Join(apurls, ",")
		return fmt.Errorf("--initial-cluster has %s but missing from --initial-advertise-peer-urls=%s (%v)", mstr, apStr, err)
	}

	for url := range apMap {
		if _, ok := initMap[url]; !ok {
			missing = append(missing, url)
		}
	}
	if len(missing) > 0 {
		mstr := strings.Join(missing, ",")
		umap := types.URLsMap(map[string]types.URLs{c.Name: c.PeerURLs})
		return fmt.Errorf("--initial-advertise-peer-urls has %s but missing from --initial-cluster=%s", mstr, umap.String())
	}

	// resolved URLs from "--initial-advertise-peer-urls" and "--initial-cluster" did not match or failed
	apStr := strings.Join(apurls, ",")
	umap := types.URLsMap(map[string]types.URLs{c.Name: c.PeerURLs})
	return fmt.Errorf("failed to resolve %s to match --initial-cluster=%s (%v)", apStr, umap.String(), err)
}

func (c *ServerConfig) MemberDir() string { return filepath.Join(c.DataDir, "member") }

func (c *ServerConfig) WALDir() string {
	if c.DedicatedWALDir != "" {
		return c.DedicatedWALDir
	}
	return filepath.Join(c.MemberDir(), "wal")
}

func (c *ServerConfig) SnapDir() string { return filepath.Join(c.MemberDir(), "snap") }

func (c *ServerConfig) ShouldDiscover() bool { return c.DiscoveryURL != "" }

// ReqTimeout returns timeout for request to finish.
// ReqTimeout返回完成一个request的时间
func (c *ServerConfig) ReqTimeout() time.Duration {
	// 5s for queue waiting, computation and disk IO delay
	// + 2 * election timeout for possible leader election
	return 5*time.Second + 2*time.Duration(c.ElectionTicks*int(c.TickMs))*time.Millisecond
}

func (c *ServerConfig) electionTimeout() time.Duration {
	return time.Duration(c.ElectionTicks*int(c.TickMs)) * time.Millisecond
}

func (c *ServerConfig) peerDialTimeout() time.Duration {
	// 1s for queue wait and election timeout
	return time.Second + time.Duration(c.ElectionTicks*int(c.TickMs))*time.Millisecond
}

func (c *ServerConfig) PrintWithInitial() { c.print(true) }

func (c *ServerConfig) Print() { c.print(false) }

func (c *ServerConfig) print(initial bool) {
	plog.Infof("name = %s", c.Name)
	if c.ForceNewCluster {
		plog.Infof("force new cluster")
	}
	plog.Infof("data dir = %s", c.DataDir)
	plog.Infof("member dir = %s", c.MemberDir())
	if c.DedicatedWALDir != "" {
		plog.Infof("dedicated WAL dir = %s", c.DedicatedWALDir)
	}
	plog.Infof("heartbeat = %dms", c.TickMs)
	plog.Infof("election = %dms", c.ElectionTicks*int(c.TickMs))
	plog.Infof("snapshot count = %d", c.SnapCount)
	if len(c.DiscoveryURL) != 0 {
		plog.Infof("discovery URL= %s", c.DiscoveryURL)
		if len(c.DiscoveryProxy) != 0 {
			plog.Infof("discovery proxy = %s", c.DiscoveryProxy)
		}
	}
	plog.Infof("advertise client URLs = %s", c.ClientURLs)
	if initial {
		plog.Infof("initial advertise peer URLs = %s", c.PeerURLs)
		plog.Infof("initial cluster = %s", c.InitialPeerURLsMap)
	}
}

func checkDuplicateURL(urlsmap types.URLsMap) bool {
	um := make(map[string]bool)
	for _, urls := range urlsmap {
		for _, url := range urls {
			u := url.String()
			if um[u] {
				return true
			}
			um[u] = true
		}
	}
	return false
}

func (c *ServerConfig) bootstrapTimeout() time.Duration {
	if c.BootstrapTimeout != 0 {
		return c.BootstrapTimeout
	}
	return time.Second
}

func (c *ServerConfig) backendPath() string { return filepath.Join(c.SnapDir(), "db") }
