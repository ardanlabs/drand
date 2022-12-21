package beacon

import (
	"context"
	"time"

	clock "github.com/jonboulle/clockwork"

	"github.com/drand/drand/chain"
)

const tickerChanBacklog = 5

type ticker struct {
	ctx     context.Context
	clock   clock.Clock
	period  time.Duration
	genesis int64
	newCh   chan channelInfo
}

func newTicker(ctx context.Context, c clock.Clock, period time.Duration, genesis int64) *ticker {
	t := &ticker{
		ctx:     ctx,
		clock:   c,
		period:  period,
		genesis: genesis,
		newCh:   make(chan channelInfo, tickerChanBacklog),
	}
	go t.Start()
	return t
}

func (t *ticker) Channel() chan roundInfo {
	newCh := make(chan roundInfo, 1)
	t.newCh <- channelInfo{
		ch:      newCh,
		startAt: t.clock.Now().Unix(),
	}
	return newCh
}

func (t *ticker) ChannelAt(start int64) chan roundInfo {
	newCh := make(chan roundInfo, 1)
	t.newCh <- channelInfo{
		ch:      newCh,
		startAt: start,
	}
	return newCh
}

func (t *ticker) CurrentRound() uint64 {
	return chain.CurrentRound(t.clock.Now().Unix(), t.period, t.genesis)
}

// Start will sleep until the next upcoming round and start sending out the
// ticks asap
func (t *ticker) Start() {
	chanTime := make(chan time.Time, 1)
	// whole reason of this function is to accept new incoming channels while
	// still sleeping until the next time
	go func() {
		select {
		case <-t.ctx.Done():
			return
		default:
		}
		now := t.clock.Now().Unix()
		_, ttime := chain.NextRound(now, t.period, t.genesis)
		if ttime > now {
			t.clock.Sleep(time.Duration(ttime-now) * time.Second)
		}
		// first tick happens at specified time
		chanTime <- t.clock.Now()
		ticker := t.clock.NewTicker(t.period)
		defer ticker.Stop()
		tickChan := ticker.Chan()
		for {
			select {
			case nt := <-tickChan:
				chanTime <- nt
			case <-t.ctx.Done():
				return
			}
		}
	}()
	var channels []channelInfo
	defer func() {
		for _, ch := range channels {
			close(ch.ch)
		}
	}()

	var sendTicks = false
	var ttime int64
	var tround uint64
	for {
		if sendTicks {
			sendTicks = false
			info := roundInfo{
				round: tround,
				time:  ttime,
			}
			for _, chinfo := range channels {
				if chinfo.startAt > ttime {
					continue
				}
				select {
				case <-t.ctx.Done():
					return
				case chinfo.ch <- info:
				default:
					// pass on, do not send if channel is full
				}
			}
		}
		select {
		case nt := <-chanTime:
			tround = chain.CurrentRound(nt.Unix(), t.period, t.genesis)
			ttime = nt.Unix()
			sendTicks = true
		case newChan := <-t.newCh:
			channels = append(channels, newChan)
		case <-t.ctx.Done():
			return
		}
	}
}

type roundInfo struct {
	round uint64
	time  int64
}

type channelInfo struct {
	ch      chan roundInfo
	startAt int64
}
