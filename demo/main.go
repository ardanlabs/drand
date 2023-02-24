package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/drand/drand/chain"
	"github.com/drand/drand/crypto"
	"github.com/drand/drand/demo/cfg"
	"github.com/drand/drand/demo/lib"
	"github.com/drand/drand/test"
)

func installDrand() {
	fmt.Println("[+] Building & installing drand")
	curr, err := os.Getwd()
	checkErr(err)
	checkErr(os.Chdir("../"))
	install := exec.Command("go", "install")
	runCommand(install)
	checkErr(os.Chdir(curr))
}

var build = flag.Bool("build", false, "Build the drand binary first.")
var binaryF = flag.String("binary", "drand", "Path to drand binary.")
var testF = flag.Bool("test", false, "Run it as a test that finishes.")
var tls = flag.Bool("tls", true, "Run the nodes with self signed certs.")
var noCurl = flag.Bool("nocurl", false, "Skip commands using curl.")
var debug = flag.Bool("debug", false, "Prints the log when panic occurs.")
var dbEngineType = flag.String("dbtype", "bolt", "Which database engine to use. Supported values: bolt, postgres, or memdb.")

func main() {
	flag.Parse()
	if *build {
		installDrand()
	}
	if *testF {
		defer func() { fmt.Println("[+] Leaving test - all good") }()
	}

	err := os.Setenv("DRAND_TEST_LOGS", "")
	checkErr(err)

	if chain.StorageType(*dbEngineType) == chain.PostgreSQL {
		stopContainer := cfg.BootContainer()
		defer stopContainer()
	}

	nRound, n := 2, 6
	thr, newThr := 4, 5
	period := "3s"
	sch, err := crypto.GetSchemeFromEnv()
	if err != nil {
		panic(err)
	}
	beaconID := test.GetBeaconIDFromEnv()

	c := cfg.Config{
		N:            n,
		Thr:          thr,
		Period:       period,
		WithTLS:      *tls,
		Binary:       *binaryF,
		WithCurl:     !*noCurl,
		Scheme:       sch,
		BeaconID:     beaconID,
		IsCandidate:  true,
		DBEngineType: chain.StorageType(*dbEngineType),
		PgDSN:        cfg.ComputePgDSN(chain.StorageType(*dbEngineType)),
		MemDBSize:    2000,
	}
	orch := lib.NewOrchestrator(c)
	// NOTE: this line should be before "StartNewNodes". The reason it is here
	// is that we are using self signed certificates, so when the first drand nodes
	// start, they need to know about all self signed certificates. So we create
	// already the new nodes here, such that when calling "StartCurrentNodes",
	// the drand nodes will load all of them already.
	orch.SetupNewNodes(3)

	defer orch.Shutdown()
	defer func() {
		// print logs in case things panic
		if err := recover(); err != nil {
			if *debug {
				fmt.Println(err)
				orch.PrintLogs()
			}
			os.Exit(1)
		}
	}()
	setSignal(orch)
	orch.StartCurrentNodes()
	orch.RunDKG(4 * time.Second)
	orch.WaitGenesis()
	for i := 0; i < nRound; i++ {
		orch.WaitPeriod()
		orch.CheckCurrentBeacon()
	}
	// stop a node and look if the beacon still continues
	nodeToStop := 3
	orch.StopNodes(nodeToStop)
	for i := 0; i < nRound; i++ {
		orch.WaitPeriod()
		orch.CheckCurrentBeacon(nodeToStop)
	}

	// stop the whole network, wait a bit and see if it can restart at the right
	// round
	/*orch.StopAllNodes(nodeToStop)*/
	// orch.WaitPeriod()
	// orch.WaitPeriod()
	// // start all but the one still down
	// orch.StartCurrentNodes(nodeToStop)
	// // leave time to network to sync
	// periodD, _ := time.ParseDuration(period)
	// orch.Wait(time.Duration(2) * periodD)
	// for i := 0; i < nRound; i++ {
	// orch.WaitPeriod()
	// orch.CheckCurrentBeacon(nodeToStop)
	// }

	// stop only more than a threshold of the network, wait a bit and see if it
	// can restart at the right round correctly
	/*nodesToStop := []int{1, 2}*/
	// fmt.Printf("[+] Stopping more than threshold of nodes (1,2,3)\n")
	// orch.StopNodes(nodesToStop...)
	// orch.WaitPeriod()
	// orch.WaitPeriod()
	// fmt.Printf("[+] Trying to start them again and check beacons\n")
	// orch.StartNode(nodesToStop...)
	orch.StartNode(nodeToStop)
	orch.WaitPeriod()
	orch.WaitPeriod()
	// at this point node should have catched up
	for i := 0; i < nRound; i++ {
		orch.WaitPeriod()
		orch.CheckCurrentBeacon()
	}

	// --- RESHARING PART ---
	orch.StartNewNodes()
	// exclude first node
	orch.CreateResharingGroup(1, newThr)
	orch.RunResharing("4s")
	orch.WaitTransition()
	limit := 10000
	if *testF {
		limit = 4
	}
	// look if beacon is still up even with the nodeToExclude being offline
	for i := 0; i < limit; i++ {
		orch.WaitPeriod()
		orch.CheckNewBeacon()
	}
}

func findTransitionTime(period time.Duration, genesis int64, secondsFromNow int64) int64 {
	transition := genesis
	for transition < time.Now().Unix()+secondsFromNow {
		transition += int64(period.Seconds())
	}
	return transition
}

func setSignal(orch *lib.Orchestrator) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		s := <-sigc
		fmt.Println("[+] Received signal ", s.String())
		orch.PrintLogs()
		orch.Shutdown()
	}()
}

func runCommand(c *exec.Cmd, add ...string) []byte {
	out, err := c.CombinedOutput()
	if err != nil {
		if len(add) > 0 {
			fmt.Printf("[-] Msg failed command: %s\n", add[0])
		}
		fmt.Printf("[-] Command \"%s\" gave\n%s\n", strings.Join(c.Args, " "), string(out))
		panic(err)
	}
	return out
}

func checkErr(err error, out ...string) {
	if err == nil {
		return
	}
	if len(out) > 0 {
		panic(fmt.Errorf("%s: %v", out[0], err))
	}

	panic(err)
}
