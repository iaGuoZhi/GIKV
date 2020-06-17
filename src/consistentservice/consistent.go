package consistentservice

import (
	"errors"
	"hash/crc32"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type uints []uint32

// uints interfaces to implement sort
// Len returns the length of the uints array.
func (x uints) Len() int { return len(x) }

// Less returns true if element i is less than element j.
func (x uints) Less(i, j int) bool { return x[i] < x[j] }

// Swap exchanges elements i and j.
func (x uints) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

// ErrEmptyCircle is the error returned when trying to get an element when nothing has been added to hash.
var ErrEmptyCircle = errors.New("empty circle")

// Consistent holds the information about the members of the consistent hash circle.
type Consistent struct {
	circle           map[uint32]string
	members          map[string]bool
	sortedHashes     uints
	NumberOfReplicas int
	count            int64
	Mu               sync.RWMutex
}

// New ... creates a new Consistent object with a default setting of 20 replicas for each entry.
func New() *Consistent {
	c := new(Consistent)
	c.NumberOfReplicas = 20
	c.circle = make(map[uint32]string)
	c.members = make(map[string]bool)
	return c
}

// Size ... return node number
func (c *Consistent) Size() int {
	return int(c.count)
}

// eltKey generates a string key for an element with an index.
func (c *Consistent) eltKey(elt string, idx int) string {
	// return elt + "|" + strconv.Itoa(idx)
	return strconv.Itoa(idx) + elt
}

// Add ... inserts a string element in the consistent hash.
func (c *Consistent) Add(elt string) {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	c.add(elt)
}

func (c *Consistent) add(elt string) {
	for i := 0; i < c.NumberOfReplicas; i++ {
		c.circle[c.hashKey(c.eltKey(elt, i))] = elt
	}
	c.members[elt] = true
	c.updateSortedHashes()
	c.count++
}

// Remove ... removes an element from the hash.
func (c *Consistent) Remove(elt string) {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	c.remove(elt)
}

func (c *Consistent) remove(elt string) {
	for i := 0; i < c.NumberOfReplicas; i++ {
		delete(c.circle, c.hashKey(c.eltKey(elt, i)))
	}
	delete(c.members, elt)
	c.updateSortedHashes()
	c.count--
}

// Get returns an element close to where name hashes to in the circle.
func (c *Consistent) Get(name string) (string, error) {
	c.Mu.RLock()
	defer c.Mu.RUnlock()
	if len(c.circle) == 0 {
		return "", ErrEmptyCircle
	}
	key := c.hashKey(name)
	i := c.search(key)
	return c.circle[c.sortedHashes[i]], nil
}

func (c *Consistent) search(key uint32) (i int) {
	f := func(x int) bool {
		return c.sortedHashes[x] > key
	}
	i = sort.Search(len(c.sortedHashes), f)
	if i >= len(c.sortedHashes) {
		i = 0
	}
	return
}

// GetN returns the N closest distinct elements to the name input in the circle.
func (c *Consistent) GetN(name string, n int) ([]string, error) {
	c.Mu.RLock()
	defer c.Mu.RUnlock()

	if len(c.circle) == 0 {
		return nil, ErrEmptyCircle
	}

	if c.count < int64(n) {
		n = int(c.count)
	}

	var (
		key   = c.hashKey(name)
		i     = c.search(key)
		start = i
		res   = make([]string, 0, n)
		elem  = c.circle[c.sortedHashes[i]]
	)

	res = append(res, elem)

	if len(res) == n {
		return res, nil
	}

	for i = start + 1; i != start; i++ {
		if i >= len(c.sortedHashes) {
			i = 0
		}
		elem = c.circle[c.sortedHashes[i]]
		if !sliceContainsMember(res, elem) {
			res = append(res, elem)
		}
		if len(res) == n {
			break
		}
	}

	return res, nil
}

// GetNext look for key, return physical node next to node in param
func (c *Consistent) GetNext(name string, node string) (string, error) {
	c.Mu.RLock()
	defer c.Mu.RUnlock()

	if len(c.circle) == 0 {
		return "", ErrEmptyCircle
	}

	var (
		key     = c.hashKey(name)
		i       = c.search(key)
		start   = i
		elem    = c.circle[c.sortedHashes[i]]
		visited = make([]string, 0, int(c.count))
		found   = false
	)

	if strings.Compare(elem, node) == 0 {
		found = true
	}
	visited = append(visited, elem)

	for i = start + 1; i != start; i++ {
		if i >= len(c.sortedHashes) {
			i = 0
		}
		elem = c.circle[c.sortedHashes[i]]

		if !sliceContainsMember(visited, elem) {
			visited = append(visited, elem)

			if found == true && strings.Compare(elem, node) != 0 {
				return elem, nil
			}
			if strings.Compare(elem, node) == 0 {
				found = true
			}
		}
	}

	return "", nil
}

func (c *Consistent) hashKey(key string) uint32 {
	return c.hashKeyCRC32(key)
}

func (c *Consistent) hashKeyCRC32(key string) uint32 {
	if len(key) < 64 {
		var scratch [64]byte
		copy(scratch[:], key)
		return crc32.ChecksumIEEE(scratch[:len(key)])
	}
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *Consistent) hashKeyFnv(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func (c *Consistent) updateSortedHashes() {
	hashes := c.sortedHashes[:0]
	//reallocate if we're holding on to too Much (1/4th)
	if cap(c.sortedHashes)/(c.NumberOfReplicas*4) > len(c.circle) {
		hashes = nil
	}
	for k := range c.circle {
		hashes = append(hashes, k)
	}
	sort.Sort(hashes)
	c.sortedHashes = hashes
}

func sliceContainsMember(set []string, member string) bool {
	for _, m := range set {
		if m == member {
			return true
		}
	}
	return false
}
