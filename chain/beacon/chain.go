package beacon

import (
	"context"
	"errors"
	"fmt"

	"github.com/drand/drand/chain"
	"github.com/drand/drand/key"
	"github.com/drand/drand/log"
	"github.com/drand/drand/net"
	"github.com/drand/drand/protobuf/drand"
)

const (
	defaultPartialChanBuffer = 10
	defaultNewBeaconBuffer   = 100
)

// chainStore implements CallbackStore, Syncer and deals with reconstructing the
// beacons, and sync when needed. This struct is the gateway logic for beacons to
// be inserted in the database and for replying to beacon requests.
type chainStore struct {
	CallbackStore
	ctx      context.Context
	l        log.Logger
	conf     *Config
	client   net.ProtocolClient
	syncm    *SyncManager
	verifier *chain.Verifier
	crypto   *cryptoStore
	ticker   *ticker
	newPartials chan partialInfo
	// catchupBeacons is used to notify the Handler when a node has aggregated a
	// beacon.
	catchupBeacons chan *chain.Beacon
	// all beacons finally inserted into the store are sent over this cannel for
	// the aggregation loop to know
	beaconStoredAgg chan *chain.Beacon
}

func newChainStore(ctx context.Context, l log.Logger, cf *Config, cl net.ProtocolClient, c *cryptoStore, store chain.Store, t *ticker) *chainStore {
	// we make sure the chain is increasing monotonically
	as := newAppendStore(store)

	// we add a store to run some checks depending on scheme-related config
	ss := NewSchemeStore(as, cf.Group.Scheme)

	// we write some stats about the timing when new beacon is saved
	ds := newDiscrepancyStore(ss, l, c.GetGroup(), cf.Clock)

	// we can register callbacks on it
	cbs := NewCallbackStore(ctx, ds)

	// we give the final append store to the sync manager
	syncm := NewSyncManager(&SyncConfig{
		Log:         l,
		Store:       cbs,
		BoltdbStore: store,
		Info:        c.chain,
		Client:      cl,
		Clock:       cf.Clock,
		NodeAddr:    cf.Public.Address(),
	})
	go syncm.Run(ctx)

	verifier := chain.NewVerifier(cf.Group.Scheme)

	cs := &chainStore{
		CallbackStore: cbs,
		ctx:           ctx,
		l:             l,
		conf:          cf,
		client:        cl,
		syncm:         syncm,
		verifier:      verifier,
		crypto:        c,
		ticker:        t,
		newPartials:     make(chan partialInfo, defaultPartialChanBuffer),
		catchupBeacons:  make(chan *chain.Beacon, 1),
		beaconStoredAgg: make(chan *chain.Beacon, defaultNewBeaconBuffer),
	}
	// we add callbacks to notify each time a final beacon is stored on the
	// database so to update the latest view
	cbs.AddCallback("chainstore", func(b *chain.Beacon) {
		cs.beaconStoredAgg <- b
	})
	// TODO maybe look if it's worth having multiple workers there
	go cs.runAggregator()
	return cs
}

func (c *chainStore) NewValidPartial(addr string, p *drand.PartialBeaconPacket) {
	c.newPartials <- partialInfo{
		addr: addr,
		p:    p,
	}
}

func (c *chainStore) Stop() {
	c.CallbackStore.Close(c.ctx)
}

// we store partials that are up to this amount of rounds more than the last
// beacon we have - it is useful to store partials that may come in advance,
// especially in case of a quick catchup.
var partialCacheStoreLimit = 3

// runAggregator runs a continuous loop that tries to aggregate partial
// signatures when it can
func (c *chainStore) runAggregator() {
	lastBeacon, err := c.Last(c.ctx)
	// TODO: This does not solve the actual issue here.
	//  For some reason, the test we are interested in, TestRunDKGReshareAbsentNode,
	//  still reproduces the error we need to fix.
	//  Further investigation is required to determine what holds `runAggregator` from running
	//  or why is it running multiple times (if it is).
	//  E.g. of the failure:
	//    2022-12-22T01:27:45.422+0200    DEBUG   127.0.0.1:39779.default.1       beacon/node.go:234              {"new_node": "following chain", "to_round": 3}
	//    2022-12-22T01:27:45.422+0200    INFO    127.0.0.1:39779.default.1       core/drand_beacon.go:284                {"transition_new": "done"}
	//    2022-12-22T01:27:45.422+0200    DEBUG   127.0.0.1:39779.default.1       beacon/node.go:306              {"run_round": "wait", "until": 449884814}
	//    2022-12-22T01:27:45.422+0200    INFO    127.0.0.1:39779.default.1       beacon/node.go:443      beacon handler stopped  {"time": "1984-04-04T00:00:12.000Z"}
	//    2022-12-22T01:27:45.422+0200    DEBUG   127.0.0.1:39779.default.1       beacon/node.go:370              {"beacon_loop": "finished"}
	//    2022-12-22T01:27:45.422+0200    DEBUG   127.0.0.1:39779.default.1.SyncManager   beacon/sync_manager.go:119      unable to fetch from store      {"sync_manager": "store.Last", "err": "database not open"}
	//    2022-12-22T01:27:45.422+0200    WARN    127.0.0.1:39779.default.1       beacon/chain.go:118             {"chain_aggregator": "loading aborted"}
	//    2022-12-22T01:27:45.422+0200    INFO    127.0.0.1:39779.default.1.SyncManager   beacon/sync_manager.go:147              {"sync_manager": "exits"}
	if err != nil {
		ctxErr := c.ctx.Err()

		if ctxErr != nil ||
			errors.Is(err, context.Canceled) {
			c.l.Warnw("", "chain_aggregator", "loading aborted")
			return
		}

		c.l.Fatalw("", "chain_aggregator", "loading", "last_beacon", ctxErr)
	}

	var cache = newPartialCache(c.l)
	for {
		select {
		case <-c.ctx.Done():
			return
		case lastBeacon = <-c.beaconStoredAgg:
			cache.FlushRounds(lastBeacon.Round)
		case partial := <-c.newPartials:
			// look if we have info for this round first
			pRound := partial.p.GetRound()
			// look if we want to store ths partial anyway
			isNotInPast := pRound > lastBeacon.Round
			isNotTooFar := pRound <= lastBeacon.Round+uint64(partialCacheStoreLimit+1)
			shouldStore := isNotInPast && isNotTooFar
			// check if we can reconstruct
			if !shouldStore {
				c.l.Debugw("", "ignoring_partial", partial.p.GetRound(), "last_beacon_stored", lastBeacon.Round)
				break
			}
			// NOTE: This line means we can only verify partial signatures of
			// the current group we are in as only current members should
			// participate in the randomness generation. Previous beacons can be
			// verified using the single distributed public key point from the
			// crypto store.
			thr := c.crypto.GetGroup().Threshold
			n := c.crypto.GetGroup().Len()
			cache.Append(partial.p)
			roundCache := cache.GetRoundCache(partial.p.GetRound(), partial.p.GetPreviousSig())
			if roundCache == nil {
				c.l.Errorw("", "store_partial", partial.addr, "no_round_cache", partial.p.GetRound())
				break
			}

			c.l.Debugw("", "store_partial", partial.addr,
				"round", roundCache.round, "len_partials", fmt.Sprintf("%d/%d", roundCache.Len(), thr))
			if roundCache.Len() < thr {
				break
			}

			msg := c.verifier.DigestMessage(roundCache.round, roundCache.prev)

			finalSig, err := key.Scheme.Recover(c.crypto.GetPub(), msg, roundCache.Partials(), thr, n)
			if err != nil {
				c.l.Errorw("invalid_recovery", "error", err, "round", pRound, "got", fmt.Sprintf("%d/%d", roundCache.Len(), n))
				break
			}
			if err := key.Scheme.VerifyRecovered(c.crypto.GetPub().Commit(), msg, finalSig); err != nil {
				c.l.Errorw("invalid_sig", "error", err, "round", pRound)
				break
			}
			cache.FlushRounds(partial.p.GetRound())

			newBeacon := &chain.Beacon{
				Round:       roundCache.round,
				PreviousSig: roundCache.prev,
				Signature:   finalSig,
			}

			c.l.Infow("", "aggregated_beacon", newBeacon.Round)
			if c.tryAppend(lastBeacon, newBeacon) {
				lastBeacon = newBeacon
				break
			}
			// XXX store them for future usage if it's a later round than what we have
			c.l.Debugw("", "new_aggregated", "not_appendable", "last", lastBeacon.String(), "new", newBeacon.String())
			if c.shouldSync(lastBeacon, newBeacon) {
				peers := toPeers(c.crypto.GetGroup().Nodes)
				c.syncm.RequestSync(newBeacon.Round, peers)
			}
		}
	}
}

func (c *chainStore) tryAppend(last, newB *chain.Beacon) bool {
	if last.Round+1 != newB.Round {
		// quick check before trying to compare bytes
		return false
	}

	if err := c.CallbackStore.Put(context.Background(), newB); err != nil {
		// if round is ok but bytes are different, error will be raised
		c.l.Errorw("", "chain_store", "error storing beacon", "err", err)
		return false
	}
	select {
	// only send if it's not full already
	case c.catchupBeacons <- newB:
	default:
		c.l.Debugw("", "chain_store", "catchup", "channel", "full")
	}
	return true
}

type likeBeacon interface {
	GetRound() uint64
}

func (c *chainStore) shouldSync(last *chain.Beacon, newB likeBeacon) bool {
	// we should sync if we are two blocks late
	return newB.GetRound() > last.GetRound()+1
}

// RunSync will sync up with other nodes and fill the store.
// It will start from the latest stored beacon. If upTo is equal to 0, then it
// will follow the chain indefinitely. If peers is nil, it uses the peers of
// the current group.
func (c *chainStore) RunSync(upTo uint64, peers []net.Peer) {
	if len(peers) == 0 {
		peers = toPeers(c.crypto.GetGroup().Nodes)
	}

	c.syncm.RequestSync(upTo, peers)
}

// RunReSync will sync up with other nodes to repair the invalid beacons in the store.
func (c *chainStore) RunReSync(ctx context.Context, faultyBeacons []uint64, peers []net.Peer, cb func(r, u uint64)) error {
	// we do this check here because the SyncManager doesn't have the notion of group
	if len(peers) == 0 {
		peers = toPeers(c.crypto.GetGroup().Nodes)
	}

	return c.syncm.CorrectPastBeacons(ctx, faultyBeacons, peers, cb)
}

// ValidateChain asks the sync manager to check the chain store up to the given beacon, in order to find invalid beacons
// and it returns the list of round numbers for which the beacons were corrupted / invalid / not found in the store.
// Note: it does not attempt to correct or fetch these faulty beacons.
func (c *chainStore) ValidateChain(ctx context.Context, upTo uint64, cb func(r, u uint64)) ([]uint64, error) {
	return c.syncm.CheckPastBeacons(ctx, upTo, cb)
}

func (c *chainStore) AppendedBeaconNoSync() chan *chain.Beacon {
	return c.catchupBeacons
}

type partialInfo struct {
	addr string
	p    *drand.PartialBeaconPacket
}

func toPeers(nodes []*key.Node) []net.Peer {
	peers := make([]net.Peer, len(nodes))
	for i := 0; i < len(nodes); i++ {
		peers[i] = nodes[i].Identity
	}
	return peers
}
