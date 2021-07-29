package rink

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"hash/fnv"
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/sebdah/goldie/v2"
)

//go:generate go test . -run TestDistribution -update -clean

func TestDistribution(t *testing.T) {
	t.Skip("Skipped since it just visualises distribution of different hash functions. Unskip to see")

	hashers := map[string]hasher{
		"md5":    wrap64(md5.New),
		"fnv":    fnv.New64,
		"fnva":   fnv.New64a, // <- Chosen as default hasher
		"sha1":   wrap64(sha1.New),
		"sha256": wrap64(sha256.New),
	}

	for name, h := range hashers {
		assertDist(t, h, name+"-emoji-few", 8, "💦", "🐒", "🦊", "🦄", "🐸", "🦋")
		assertDist(t, h, name+"-emoji-many", 8, "🙈", "💥", "💫", "🔥", "🏄", "🐻", "🦄", "💦", "🐑", "🐸", "🐽", "❤️", "🌍", "🐼", "🐦", "🌸")
		assertDist(t, h, name+"-ints-few", 8, "1", "2", "3", "4", "5", "6", "7")
		assertDist(t, h, name+"-ints-many", 8, "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20")
		assertDist(t, h, name+"-pairs-few", 8, "btcusd", "btceur", "ethbtc", "ethusd", "etheur", "xrpbtc", "xrpeth")
	}

}

func assertDist(t *testing.T, hasher hasher, name string, max int, roles ...string) {
	t.Helper()
	s := make(state)

	type result struct {
		Size    int
		Members []Member
	}

	var results []result
	for i := 0; i < max; i++ {
		s[strconv.Itoa(i)] = i
		size := len(s)

		result := result{Size: size}
		for i := 0; i < size; i++ {
			name := strconv.Itoa(i)

			var member Member
			for _, role := range roles {
				if hasRole(s, hasher, name, role) {
					member.Roles = append(member.Roles, role)
				}
			}
			result.Members = append(result.Members, member)
		}
		results = append(results, result)
	}

	goldie.New(t).AssertJson(t, path.Join(t.Name(), name), results)
}

type Member struct {
	Roles []string
}

func (m Member) MarshalJSON() ([]byte, error) {
	return []byte("\"" + strings.Join(m.Roles, ",") + "\""), nil
}

type w64 struct {
	hash.Hash
}

func (w w64) Sum64() uint64 {
	return binary.BigEndian.Uint64(w.Sum(nil))
}

func wrap64(f func() hash.Hash) func() hash.Hash64 {
	return func() hash.Hash64 {
		return w64{f()}
	}
}
