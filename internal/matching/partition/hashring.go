package partition

import (
	"hash/fnv"
	"sort"
	"strconv"
)

type Ring struct {
	nodes    []uint32
	nodeMap  map[uint32]int32
	replicas int
}

func NewRing(replicas int) *Ring {
	return &Ring{
		nodes:    make([]uint32, 0),
		nodeMap:  make(map[uint32]int32),
		replicas: replicas,
	}
}

func (r *Ring) Add(partitionID int32) {
	for i := 0; i < r.replicas; i++ {
		key := strconv.Itoa(int(partitionID)) + "-" + strconv.Itoa(i)
		h := r.hash(key)

		// Handle collisions: simple linear probing
		for {
			if _, exists := r.nodeMap[h]; !exists {
				break
			}
			h++
		}

		r.nodes = append(r.nodes, h)
		r.nodeMap[h] = partitionID
	}
	sort.Slice(r.nodes, func(i, j int) bool {
		return r.nodes[i] < r.nodes[j]
	})
}

func (r *Ring) Get(key string) int32 {
	if len(r.nodes) == 0 {
		return 0
	}

	h := r.hash(key)
	idx := sort.Search(len(r.nodes), func(i int) bool {
		return r.nodes[i] >= h
	})

	if idx == len(r.nodes) {
		idx = 0
	}

	return r.nodeMap[r.nodes[idx]]
}

func (r *Ring) hash(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func (r *Ring) Remove(partitionID int32) {
	newNodes := make([]uint32, 0, len(r.nodes))
	for _, node := range r.nodes {
		if r.nodeMap[node] != partitionID {
			newNodes = append(newNodes, node)
		} else {
			delete(r.nodeMap, node)
		}
	}
	r.nodes = newNodes
}
