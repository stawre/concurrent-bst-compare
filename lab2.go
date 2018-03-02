package main

import (
	"flag"
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"strconv"
	"sync"
	"time"
)

type node struct {
	value int
	left  *node
	right *node
}

type map_element struct {
	hash uint64
	bst_id int
}

var mux sync.Mutex
var hash_map = make(map[uint64][]int)

func nodeConstruct(value int) *node {
	my_node := new(node)
	my_node.value = value
	my_node.left = nil
	my_node.right = nil
	return my_node
}

func inOrderTraversal(tree []int) []int {
	retval := make([]int, 0)
	root := nodeConstruct(tree[0])
	root = makeBTree(tree, root, retval)

	computeInOrder(root, &retval)

	return retval
}

func makeBTree(tree []int, root *node, retval []int) *node {
	tree_len := len(tree)
	index := 1

	for tree_len > 1 {
		insertInBTree(root, tree[index])
		index++
		tree_len--
	}

	return root
}

func insertInBTree(root *node, element int) {
	if element > root.value {
		if root.right != nil {
			insertInBTree(root.right, element)
		} else {
			root.right = nodeConstruct(element)
		}
	} else {
		if root.left != nil {
			insertInBTree(root.left, element)
		} else {
			root.left = nodeConstruct(element)
		}
	}
}

func computeInOrder(root *node, retval *[]int) {
	if (root == nil) {
		return
	}

	computeInOrder(root.left, retval)

	*retval = append(*retval, root.value)

	computeInOrder(root.right, retval)
}

func hash(hash uint64, val int) uint64 {
	var val2 int = val + 2
	var prime uint64 = 4222234741
	return (hash * uint64(val2) + uint64(val2)) % prime
}

func hashFunc(tree []int, bst_id int) {
// func hashFunc(tree []int, my_chan *chan map_element, bst_id int) {
// func hashFunc(tree []int, hashI *uint64) {
	var retval uint64 = 0
	tree_len := len(tree)
	for i := 0; i < tree_len; i++ {
		retval = hash(retval, tree[i])
	}

	// *hashI = retval

	// *my_chan <- map_element{hash: retval, bst_id: bst_id}
	mux.Lock()
	hash_map[retval] = append(hash_map[retval], bst_id)
	mux.Unlock()

	// wg.Done()
}

func parallelHashFunc(partition *[][]int, q int, wg *sync.WaitGroup, i int) {
// func parallelHashFunc(partition *[][]int, q int, tree_hash *uint64, wg *sync.WaitGroup) {
// func parallelHashFunc(partition *[][]int, q int, wg *sync.WaitGroup, my_chan *chan map_element, i int) {

	// fmt.Println(q)
	for j := 0; j < q; j++ {
		// fmt.Println(i, j)
		// wg.Add(1)

		bst_id := q*i + j

		hashFunc((*partition)[j], bst_id)
		// hashFunc((*partition)[j], my_chan, bst_id)
		if (j == q - 1) {
			wg.Done()
		}
	}
}

func compareTrees(tree1 []int, tree2 []int, wg *sync.WaitGroup, retval *bool) {
	size := len(tree1)
	for i := 0; i < size; i++ {
		if (tree1[i] != tree2[i]) {
			*retval = false
		}
	}
	wg.Done()
	*retval = true
}

func main() {
	hashWorkers := flag.Int("hash-workers", 1, "hash workers")
	dataWorkers := flag.Int("data-workers", 1, "data workers")
	compWorkers := flag.Int("comp-workers", 1, "comp workers")
	inputFile	:= flag.String("input", "input.txt", "hash workers")

	flag.Parse()

	fmt.Println("hash:", *hashWorkers)
	fmt.Println("data:", *dataWorkers)
	fmt.Println("comp:", *compWorkers)
	fmt.Println("input:", *inputFile)

	trees := make([][]int, 0)

	file, err := os.Open(*inputFile)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	tree_index := 0
	node_index := 0

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		curr_string := scanner.Text()
		curr_scanner := bufio.NewScanner(strings.NewReader(curr_string))
		curr_scanner.Split(bufio.ScanWords)
		node_index = 0
		my_tree := make([]int, 0)
		trees = append(trees, my_tree)
		for curr_scanner.Scan() {
			num, err := strconv.Atoi(curr_scanner.Text())
			if err != nil {
				log.Fatal(err)
			}
			trees[tree_index] = append(trees[tree_index], num)
			node_index++
		}
		tree_index++
	}

	// start := time.Now()

	tree_size := len(trees)
	tree_dim := len(trees[0])
	inOrderTrees := make([][]int, tree_size)

	for i := range inOrderTrees {
		inOrderTrees[i] = make([]int, tree_dim)
	}

	for i := 0; i < tree_size; i++ {
		inOrderTrees[i] = inOrderTraversal(trees[i])
	}

	// tree_hashes := make([]uint64, tree_size)
	// tree_map := make(map[int][]int)

	var wg sync.WaitGroup
	equality := make([][]bool, tree_size)

	for i := range equality {
		equality[i] = make([]bool, tree_size)
	}

	// start1 := time.Now()
	//
	// for i := 0; i < tree_size; i++ {
	// 	wg.Add(1)
	// 	go hashFunc(trees[i], &tree_hashes[i], &wg)
	// }
	//
	// wg.Wait()
	//
	// elapsed1 := time.Since(start1)

	/* Thread pool implementation */

	q := tree_size / *hashWorkers
	// fmt.Println(q)
	r := tree_size % *hashWorkers

	trees_partitions := make([][][]int, *hashWorkers)
	for i := range trees_partitions {
		trees_partitions[i] = make([][]int, q)
		for j := range trees_partitions[i] {
			trees_partitions[i][j] = make([]int, tree_dim)
		}
	}

	counter := 0
	for i := 0; i < *hashWorkers; i++ {
		for j := 0; j < q; j++ {
			for k := 0; k < tree_dim; k++ {
				trees_partitions[i][j][k] = trees[counter][k]
			}
			counter++
		}
	}

	for i := 0; i < r; i++ {
		trees_partitions[i] = append(trees_partitions[i], trees[counter])
		counter++
	}

	start1 := time.Now()

	// my_chan := make(chan map_element)

	c2 := 0
	for i := 0; i < *hashWorkers; i++ {
		wg.Add(1)
		// go parallelHashFunc(&trees_partitions[i], q, &wg, &my_chan, i)
		go parallelHashFunc(&trees_partitions[i], q, &wg, i)
		// go parallelHashFunc(partition, q, &tree_hashes[i], &wg, i)
		c2++
	}

	wg.Wait()
	// hash_map := make(map[uint64][]int)

	 // go func (my_chan chan map_element){
		// for my_element := range my_chan {
		// 	hash_map[my_element.hash] = append(hash_map[my_element.hash], my_element.bst_id)
		// }
	 // }(my_chan)

	// for i := 0; i < tree_size; i++ {
	// 	for j := 0; j < tree_size; j++ {
	// 		if (tree_hashes[i] == tree_hashes[j]) {
	// 			equality[i][j] = true
	// 			if compareTrees(inOrderTrees[i], inOrderTrees[j]) {
	// 				tree_map[i] = append(tree_map[i], j)
	// 			}
	// 		}
	// 	}
	// }
	var wg1 sync.WaitGroup

	for elem := range hash_map {
		temp := hash_map[elem]
		n := len(temp)
		if (n > 1) {
			for id := range temp {
				for id2 := range temp {
					wg1.Add(1)
					var retval bool
					go compareTrees(inOrderTrees[id], inOrderTrees[id2], &wg1, &retval)
					if retval {
						equality[id][id2] = true
					}
				}
			}
		} else {
			 equality[temp[0]][temp[0]] = true
		}
	}

	wg1.Wait()
	elapsed1 := time.Since(start1)

	fmt.Printf("Time taken by 1: %s\n", elapsed1)

	// elapsed := time.Since(start)

	// for i := 0; i < 10; i++ {
	// 	fmt.Println(equality[i])
	// }

	// fmt.Printf("Time taken by 1: %s, Time taken by 2: %s\n", elapsed1, elapsed)
	// fmt.Printf("Time taken by 1: %s\n", elapsed1)

}
