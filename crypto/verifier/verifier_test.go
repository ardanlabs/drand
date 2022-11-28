package verifier

import (
	"testing"

	"github.com/drand/drand/chain"
	"github.com/drand/drand/common/scheme"
	"github.com/drand/kyber/util/random"
)

func BenchmarkVerifyBeacon(b *testing.B) {
	sch := scheme.GetSchemeFromEnv()

	secret := sch.KeyGroup.Scalar().Pick(random.New())
	public := sch.KeyGroup.Point().Mul(secret, nil)

	verifier := NewVerifier(sch)

	var round uint64 = 16
	prevSig := []byte("My Sweet Previous Signature")

	msg := verifier.DigestMessage(round, prevSig)

	sig, _ := sch.AuthScheme.Sign(secret, msg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b := chain.Beacon{
			PreviousSig: prevSig,
			Round:       16,
			Signature:   sig,
		}

		err := verifier.VerifyBeacon(b, public)
		if err != nil {
			panic(err)
		}
	}
}
