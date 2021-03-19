package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	crand "crypto/rand"

	"github.com/cheggaaa/pb/v3"
	lmdb "github.com/filecoin-project/go-bs-lmdb"
	"github.com/ipfs/go-bitswap"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs/importer"
	"github.com/libp2p/go-libp2p"
	connmgr "github.com/libp2p/go-libp2p-connmgr"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	crypto "github.com/libp2p/go-libp2p-crypto"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/xerrors"

	cli "github.com/urfave/cli/v2"
)

var bootstrappers = []string{
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
}

func main() {
	app := cli.NewApp()

	app.Commands = []*cli.Command{
		scrapeCmd,
		fetchCmd,
	}

	app.RunAndExitOnError()
}

/*
{
  tokens {
    contract
    tokenID
    owner
    tokenURI
  }
}
*/

type TokenContract struct {
	Id                     string
	Name                   string
	NumOwners              string
	NumTokens              string
	SupportsEIP721Metadata bool
}

type response struct {
	Data struct {
		TokenContracts []TokenContract
	}
}

var scrapeCmd = &cli.Command{
	Name: "scrape",
	Action: func(cctx *cli.Context) error {
		cur := 0
		data := fmt.Sprintf(`{"query":"{\n  tokenContracts(orderBy: numOwners, orderDirection: desc, first: 1000, offset: %d) {\n    id\n    name\n    numTokens\n    numOwners\n    supportsEIP721Metadata\n  }\n}\n","variables":null}`, cur)

		resp, err := http.Post("https://api.thegraph.com/subgraphs/name/wighawag/eip721-subgraph", "application/json", strings.NewReader(data))
		if err != nil {
			return err
		}

		if resp.StatusCode != 200 {
			return fmt.Errorf("failed query %d %s", resp.StatusCode, resp.Status)
		}

		buf := new(bytes.Buffer)
		io.Copy(buf, resp.Body)

		var out response
		err = json.NewDecoder(buf).Decode(&out)
		if err != nil {
			return err
		}

		for _, t := range out.Data.TokenContracts {
			fmt.Printf("%s\t%s\n", t.Id, t.Name)
		}

		return nil

	},
}

type NiftyInfo struct {
	TokenID  string
	Org      string
	Contract string
	IpfsHash string
	URI      string
}

type NiftyStatus struct {
	Providers               []peer.ID
	HaveData                bool
	Failure                 string
	FetchFailure            string
	MetaDataOnIpfs          bool
	AssetOnIpfs             bool
	AvailableAfterHttpFetch bool
	OutputHash              string
	AssetUrl                string
}

type tokenInput struct {
	TokenID  string
	TokenURI string
	IPFSHash string
}

type inputData struct {
	ContractAddress string
	ContractName    string
	Tokens          []tokenInput
}

func parseInputFile(fname string) ([]*NiftyInfo, error) {
	fi, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	defer fi.Close()

	var inps []inputData
	dec := json.NewDecoder(fi)
	for {
		var inp inputData
		if err := dec.Decode(&inp); err != nil {
			if xerrors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		fmt.Println("parsed: ", inp.ContractName)
		inps = append(inps, inp)
	}

	var out []*NiftyInfo
	for _, inp := range inps {
		for _, tok := range inp.Tokens {
			out = append(out, &NiftyInfo{
				TokenID:  tok.TokenID,
				URI:      tok.TokenURI,
				Org:      inp.ContractName,
				Contract: inp.ContractAddress,
				IpfsHash: tok.IPFSHash,
			})
		}
	}

	return out, nil
}

var fetchCmd = &cli.Command{
	Name: "fetch",
	Action: func(cctx *cli.Context) error {
		fmt.Println("parsing input...")
		nifties, err := parseInputFile(cctx.Args().First())
		if err != nil {
			return err
		}

		fmt.Println("setting up node...")
		nd, err := setup(context.TODO())
		if err != nil {
			return err
		}

		for _, a := range nd.Host.Addrs() {
			fmt.Printf("%s/p2p/%s\n", a, nd.Host.ID())
		}

		fmt.Println("bootstrapping...")
		for _, bsp := range bootstrappers {

			ma, err := multiaddr.NewMultiaddr(bsp)
			if err != nil {
				fmt.Println("failed to parse bootstrap address: ", err)
				continue
			}
			ai, err := peer.AddrInfoFromP2pAddr(ma)
			if err != nil {
				fmt.Println("failed to create address info: ", err)
				continue
			}

			if err := nd.Host.Connect(context.TODO(), *ai); err != nil {
				fmt.Println("failed to connect to bootstrapper: ", err)
				continue
			}
		}

		fmt.Println("running dht bootstrap")
		if err := nd.Dht.Bootstrap(context.TODO()); err != nil {
			fmt.Println("dht bootstrapping failed: ", err)
		}

		sema := make(chan struct{}, 64)
		out := make([]*NiftyStatus, len(nifties))
		fmt.Println("fetching nifties now!")
		bar := pb.New(len(nifties)).Start()
		var wg sync.WaitGroup
		for i := range nifties {

			sema <- struct{}{}
			wg.Add(1)

			go func(ix int) {
				n := nifties[ix]

				defer func() {
					wg.Done()
					bar.Add(1)
					<-sema
				}()

				fmt.Println("fetching: ", n.URI)
				resp, err := nd.processNifty(context.TODO(), n)
				if err != nil {
					fmt.Println("Process error: ", err)
					return
				}

				out[ix] = resp
			}(i)
		}

		wg.Wait()

		fi, err := os.Create("nifty-results.csv")
		if err != nil {
			return err
		}
		defer fi.Close()

		w := csv.NewWriter(fi)
		defer w.Flush()

		if err := writeOutputHeader(w); err != nil {
			return err
		}
		for i, n := range nifties {
			if err := writeNiftyStatusOut(w, n, out[i]); err != nil {
				return err
			}
		}

		return nil
	},
}

func writeOutputHeader(w *csv.Writer) error {
	return w.Write([]string{
		"Contract",
		"IpfsHash",
		"Org",
		"URI",
		"TokenID",
		"OutputHash",
		"AssetUrl",
		"NumProviders",
		"DataOnIpfs",
		"DataAvailableAfterHttpFetch",
		"MetaDataOnIpfs",
		"AssetOnIpfs",
		"Failure",
		"IpfsGetFailure",
	})
}

func writeNiftyStatusOut(w *csv.Writer, n *NiftyInfo, r *NiftyStatus) error {
	return w.Write([]string{
		n.Contract,
		n.IpfsHash,
		n.Org,
		n.URI,
		n.TokenID,
		r.OutputHash,
		r.AssetUrl,
		fmt.Sprint(len(r.Providers)),
		fmt.Sprint(r.HaveData),
		fmt.Sprint(r.AvailableAfterHttpFetch),
		fmt.Sprint(r.MetaDataOnIpfs),
		fmt.Sprint(r.AssetOnIpfs),
		r.Failure,
		r.FetchFailure,
	})
}

func (nd *Node) processNifty(ctx context.Context, nft *NiftyInfo) (*NiftyStatus, error) {
	return nd.fetchNiftyStatus(ctx, nft)
}

type niftyMeta struct {
	Description string
	ExternalUrl string `json:"external_url"`
	Image       string
	Name        string
}

func (nd *Node) fetchIpfsData(ctx context.Context, ipfs string) ([]peer.ID, bool, error) {
	ipfshash, err := cid.Decode(ipfs)
	if err != nil {
		return nil, false, err
	}
	/*

		nd.memoLk.Lock()
		mem, ok := nd.memo[ipfshash]
		if ok {
			nd.memoLk.Unlock()
			<-mem.wait
			if mem.err != nil {
				return nil, mem.err
			}
			return mem.status, nil
		}
		mem = &activeSearch{
			wait: make(chan struct{}),
		}
		nd.memo[ipfshash] = mem
		nd.memoLk.Unlock()

		st := &NiftyStatus{}
		mem.status = st

		defer func() {
			close(mem.wait)
		}()
	*/

	pctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	provs, err := nd.Dht.FindProviders(pctx, ipfshash)
	if err != nil {
		return nil, false, err
	}

	var provids []peer.ID
	for _, p := range provs {
		provids = append(provids, p.ID)
	}

	nctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	err = merkledag.FetchGraph(nctx, ipfshash, nd.Dag)
	if err != nil {
		return provids, false, nil
	}

	return provids, true, nil
}

func (nd *Node) fetchNiftyStatus(ctx context.Context, nft *NiftyInfo) (*NiftyStatus, error) {
	st := new(NiftyStatus)

	if nft.IpfsHash != "" {
		provs, have, err := nd.fetchIpfsData(ctx, nft.IpfsHash)
		if err != nil {
			st.FetchFailure = err.Error()
		}
		st.Providers = provs
		st.HaveData = have
	}

	st.MetaDataOnIpfs = strings.Contains(nft.URI, "ipfs")
	meta, err := nd.fetchMeta(nft)
	if err != nil {
		st.Failure = fmt.Sprintf("failed to fetch meta: %s", err)
		return st, nil
	}

	st.AssetOnIpfs = strings.Contains(meta.Image, "ipfs")
	st.AssetUrl = meta.Image

	datacid, err := nd.fetchNftData(meta.Image)
	if err != nil {
		st.Failure = fmt.Sprintf("failed to fetch meta: %s", err)
		return st, nil
	}

	st.OutputHash = datacid.String()

	if nft.IpfsHash != "" && datacid.String() != nft.IpfsHash {
		st.Failure = "ipfs hash of fetched data doesnt match"
	} else {
		st.AvailableAfterHttpFetch = true
	}

	return st, nil
}

func (nd *Node) fetchNftData(url string) (cid.Cid, error) {
	if strings.HasPrefix(url, "ipfs://") {
		url = "https://ipfs.io" + url[6:]
	}

	resp, err := http.Get(url)
	if err != nil {
		return cid.Undef, err
	}

	defer resp.Body.Close()

	spl := chunker.DefaultSplitter(resp.Body)
	ind, err := importer.BuildDagFromReader(nd.Dag, spl)
	if err != nil {
		return cid.Undef, err
	}

	return ind.Cid(), nil
}

func (nft *NiftyInfo) label() string {
	return nft.Contract + nft.TokenID
}

func (nd *Node) fetchMeta(nft *NiftyInfo) (*niftyMeta, error) {
	nd.metaLk.Lock()
	f, ok := nd.metaMemo[nft.label()]
	if ok {
		nd.metaLk.Unlock()
		<-f.wait
		return f.meta, f.err
	}
	f = &metaFetch{
		wait: make(chan struct{}),
	}
	nd.metaMemo[nft.label()] = f

	nd.metaLk.Unlock()

	if strings.HasPrefix(url, "ipfs://") {
		url = "https://ipfs.io" + url[6:]
	}

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("metadata fetch request failed: %d %s", resp.StatusCode, resp.Status)
	}

	defer resp.Body.Close()

	var meta niftyMeta
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return nil, err
	}

	return &meta, nil
}

type activeSearch struct {
	wait   chan struct{}
	status *NiftyStatus
	err    error
}

type metaFetch struct {
	wait chan struct{}
	err  error
	meta *niftyMeta
}

type Node struct {
	Dht  *dht.IpfsDHT
	Host host.Host

	Datastore datastore.Batching

	Blockstore blockstore.Blockstore
	Bitswap    *bitswap.Bitswap
	Dag        ipld.DAGService

	MetaCacheDir string

	memoLk sync.Mutex
	memo   map[cid.Cid]*activeSearch

	metaLk   sync.Mutex
	metaMemo map[string]*metaFetch
}

func loadOrInitPeerKey(kf string) (crypto.PrivKey, error) {
	data, err := ioutil.ReadFile(kf)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		k, _, err := crypto.GenerateEd25519Key(crand.Reader)
		if err != nil {
			return nil, err
		}

		data, err := crypto.MarshalPrivateKey(k)
		if err != nil {
			return nil, err
		}

		if err := ioutil.WriteFile(kf, data, 0600); err != nil {
			return nil, err
		}

		return k, nil
	}
	return crypto.UnmarshalPrivateKey(data)
}

func setup(ctx context.Context) (*Node, error) {
	peerkey, err := loadOrInitPeerKey("niftyscrape.libp2p.key")
	if err != nil {
		return nil, err
	}

	h, err := libp2p.New(ctx,
		//libp2p.ListenAddrStrings(cfg.ListenAddrs...),
		libp2p.NATPortMap(),
		libp2p.ConnectionManager(connmgr.NewConnManager(500, 800, time.Minute)),
		libp2p.Identity(peerkey),
	)
	if err != nil {
		return nil, err
	}

	dht, err := dht.New(ctx, h)
	if err != nil {
		return nil, err
	}

	bstore, err := lmdb.Open(&lmdb.Options{
		Path: "allthedata",
	})
	if err != nil {
		return nil, err
	}

	bsnet := bsnet.NewFromIpfsHost(h, dht)
	bswap := bitswap.New(ctx, bsnet, bstore)

	bserv := blockservice.New(bstore, bswap)
	ds := merkledag.NewDAGService(bserv)
	return &Node{
		Dht:        dht,
		Host:       h,
		Blockstore: bstore,
		Bitswap:    bswap.(*bitswap.Bitswap),
		Dag:        ds,
		memo:       make(map[cid.Cid]*activeSearch),
	}, nil
}
