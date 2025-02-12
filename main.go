package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	bitswap "github.com/ipfs/boxo/bitswap"
	bsnet "github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	bs "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs"
	unixfsio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	ds "github.com/ipfs/go-datastore"
	dsSync "github.com/ipfs/go-datastore/sync"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	dualdht "github.com/libp2p/go-libp2p-kad-dht/dual"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multiaddr"
)

// TODO Go 1.24: use https://pkg.go.dev/os#Root

// from https://github.com/hsanjuan/ipfs-lite/blob/master/util.go
func newDHT(ctx context.Context, h host.Host, ds datastore.Batching) (*dualdht.DHT, error) {
	dhtOpts := []dualdht.Option{
		dualdht.DHTOption(dht.NamespacedValidator("pk", record.PublicKeyValidator{})),
		dualdht.DHTOption(dht.NamespacedValidator("ipns", ipns.Validator{KeyBook: h.Peerstore()})),
		dualdht.DHTOption(dht.Concurrency(10)),
		dualdht.DHTOption(dht.Mode(dht.ModeAuto)),
	}
	if ds != nil {
		dhtOpts = append(dhtOpts, dualdht.DHTOption(dht.Datastore(ds)))
	}

	return dualdht.New(ctx, h, dhtOpts...)
}

// from https://github.com/hsanjuan/ipfs-lite/blob/master/ipfs.go
func bootstrap(ctx context.Context, p host.Host, d *dualdht.DHT) {
	var bootstrapPeers []peer.AddrInfo

	for _, bsp := range dht.DefaultBootstrapPeers {
		peerInfo, err := peer.AddrInfoFromP2pAddr(bsp)
		if err != nil {
			fmt.Printf("AddrInfoFromP2pAddr err: %v", err)
		} else {
			bootstrapPeers = append(bootstrapPeers, *peerInfo)
		}
	}

	connected := make(chan struct{})

	fmt.Printf("Bootstrapping with %d peers\n", len(bootstrapPeers))

	var wg sync.WaitGroup

	for _, pinfo := range bootstrapPeers {
		wg.Add(1)

		go func(pi peer.AddrInfo) {
			defer wg.Done()
			err := p.Connect(ctx, pi)
			if err != nil {
				fmt.Printf("Could not connect to bootstrap node %v", err)
				return
			}
			p.Peerstore().AddAddrs(pi.ID, pi.Addrs, peerstore.PermanentAddrTTL)
			fmt.Printf("Connected to %v\n", pi.ID)
			connected <- struct{}{}
		}(pinfo)
	}

	go func() {
		wg.Wait()
		close(connected)
	}()

	i := 0
	for range connected {
		i++
	}

	if nPeers := len(bootstrapPeers); i < nPeers/2 {
		fmt.Printf("only connected to %d bootstrap peers out of %d\n", i, nPeers)
	}

	err := d.Bootstrap(ctx)
	if err != nil {
		fmt.Printf("error setting bootstrapped status: %s", err)
		return
	}
}

func newLibp2p(ctx context.Context, ds datastore.Batching) (host.Host, *dualdht.DHT) {
	var ddht *dualdht.DHT

	listen, err := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/0")
	if err != nil {
		fmt.Printf("cannot create listen multiaddr %s", err)
		os.Exit(1)
	}

	var transports = libp2p.DefaultTransports

	opts := []libp2p.Option{
		libp2p.ListenAddrs([]multiaddr.Multiaddr{listen}...),
		transports,
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			ddht, err = newDHT(ctx, h, ds)
			return ddht, err
		}),
	}

	host, err := libp2p.New(opts...)
	if err != nil {
		log.Fatalf("Failed to create libp2p host: %s", err)
	}

	return host, ddht
}

func setupIPFSServices(ctx context.Context, host host.Host, ddht *dualdht.DHT, bs blockstore.Blockstore) ipld.DAGService {
	// Set up Bitswap network and exchange.
	bswapNet := bsnet.NewFromIpfsHost(host)
	exchange := bitswap.New(ctx, bswapNet, ddht, bs)

	// Create a block service and a DAG service.
	blkService := blockservice.New(bs, exchange)
	return merkledag.NewDAGService(blkService)
}

func main() {
	_ = logging.Logger("cid-getter")
	logging.SetLogLevel("*", "ERROR")

	if len(os.Args) < 3 {
		fmt.Printf("Usage: %s <cid> <output-directory>\n", os.Args[0])
		os.Exit(1)
	}
	cidStr := os.Args[1]
	outDir := os.Args[2]

	c, err := cid.Parse(cidStr)
	if err != nil {
		log.Fatalf("Invalid CID: %s", err)
	}

	ctx := context.Background()
	var ddht *dualdht.DHT

	// Set up an in‑memory datastore and blockstore.
	mDatastore := dsSync.MutexWrap(ds.NewMapDatastore())
	blockstore := bs.NewBlockstore(mDatastore)

	// Create a new libp2p Host
	host, ddht := newLibp2p(ctx, mDatastore)
	defer host.Close()

	// Bootstrap the DHT
	bootstrap(ctx, host, ddht)

	log.Printf("Host ID: %s", host.ID().String())
	log.Printf("Addrs: %v", host.Network().ListenAddresses())

	dagService := setupIPFSServices(ctx, host, ddht, blockstore)
	log.Printf("Configured host for IPFS")

	log.Printf("Searching for root DAG node for CID %s...", c.String())
	ctxTO, cancelFunc := context.WithTimeout(ctx, time.Second*15)
	defer cancelFunc()

	node, err := dagService.Get(ctxTO, c)
	if err != nil {
		log.Fatalf("Failed to get root DAG node: %s", err)
	}

	log.Printf("DAG received, downloading...")

	if err := exportUnixfsNode(ctx, dagService, node, outDir); err != nil {
		log.Fatalf("Failed to export node: %s", err)
	}

	fmt.Printf("CID download completed successfully to %s\n", outDir)
}

// exportUnixfsNode recursively exports an IPFS UnixFS node to a local path.
// It handles both ProtoNodes (which may represent files or directories) and raw nodes.
func exportUnixfsNode(ctx context.Context, ds ipld.DAGService, node ipld.Node, outPath string) error {
	// If the node is a ProtoNode, try to extract its UnixFS metadata.
	if proto, ok := node.(*merkledag.ProtoNode); ok {
		// Attempt to extract UnixFS node information.
		if fsNode, err := unixfs.ExtractFSNode(proto); err == nil {
			switch fsNode.Type() {
			case unixfs.TDirectory:
				// Create the local directory.
				if err := os.MkdirAll(outPath, 0755); err != nil {
					return err
				}
				// Recursively export each link.
				for _, lnk := range proto.Links() {
					child, err := ds.Get(ctx, lnk.Cid)
					if err != nil {
						return err
					}
					childPath := filepath.Join(outPath, lnk.Name)
					if err := exportUnixfsNode(ctx, ds, child, childPath); err != nil {
						return err
					}
				}
				return nil

			case unixfs.TFile:
				// Obtain a reader for the file data.
				fileReader, err := unixfsio.NewDagReader(ctx, node, ds)
				if err != nil {
					return err
				}
				// Create the output file and copy its contents.
				outFile, err := os.Create(outPath)
				if err != nil {
					return err
				}
				defer outFile.Close()
				if _, err = io.Copy(outFile, fileReader); err != nil {
					return err
				}
				return nil

			default:
				return fmt.Errorf("unsupported UnixFS node type: %v", fsNode.Type())
			}
		}
	}

	// If the node is not a ProtoNode with UnixFS metadata, treat it as a raw node.
	rawData := node.RawData()
	if rawData == nil {
		return fmt.Errorf("node has no raw data")
	}

	return os.WriteFile(outPath, rawData, 0644)
}
