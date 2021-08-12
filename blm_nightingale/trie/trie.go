package trie

//Not thread safe !!!

import (
	"fmt"
	"strings"
)

var (
	// SingleTokenWildcard 指代一个
	SingleTokenWildcard = "?"
	// MultipleTokenWildcard 指代零个或任意多个
	MultipleTokenWildcard = "*"
	Sep                   = "."
	MaxLength             = 191
)

type Node struct {
	name     string
	parent   *Node
	children map[string]*Node
	value    *int
}

func (n *Node) Children() []string {
	var r []string
	for s := range n.children {
		r = append(r, s)
	}
	return r
}

type Trie struct {
	root *Node
}

func New() *Trie {
	return &Trie{
		root: &Node{children: make(map[string]*Node)},
	}
}

func (t *Trie) Root() *Node {
	return t.root
}

func (t *Trie) Add(key string, value int) (*Node, error) {
	if len(key) > MaxLength {
		return nil, fmt.Errorf("the length of the key cannot exceed %d", MaxLength)
	}
	if value < 0 {
		return nil, fmt.Errorf("value must be a positive integer")
	}
	paths := strings.Split(key, Sep)
	node := t.root
	for i := range paths {
		r := paths[i]
		//"*"仅允许出现在最后一个字符
		if r == MultipleTokenWildcard && i != len(paths)-1 {
			return nil, fmt.Errorf("multiple token wildcard %s must only appear at the end", MultipleTokenWildcard)
		}
		if n, ok := node.children[r]; ok {
			if i == len(paths)-1 {
				//最后一个元素检查是否value匹配上
				if n.value != nil && *n.value != value {
					return nil, fmt.Errorf("%s have dirrerent value %d and %d ", key, *n.value, value)
				}
			}
			node = n
		} else {
			if i == len(paths)-1 {
				node = node.NewChild(r, &value)
			} else {
				node = node.NewChild(r, nil)
			}
		}
	}
	return node, nil
}

// Find 存在找到但非叶子节点
func (t *Trie) Find(key string) (*int, bool) {
	if len(key) == 0 {
		return nil, false
	}
	node := findNode(t.Root(), strings.Split(key, Sep), 0)
	if node == nil {
		return nil, false
	}
	if node.value == nil {
		//定义如 A.*会产生节点 root->A->*,此时A节点value为nil, 搜索 "A"时需要向下查找看看是不是有*节点
		child, exist := node.children[MultipleTokenWildcard]
		if exist {
			return child.value, true
		}
	}
	return node.value, true
}

func (n *Node) NewChild(val string, value *int) *Node {
	node := &Node{
		name:     val,
		parent:   n,
		children: make(map[string]*Node),
	}
	if value != nil {
		node.value = value
	}
	n.children[node.name] = node
	return node
}

func findNode(node *Node, path []string, index int) *Node {
	if node == nil {
		return nil
	}
	var n *Node
	var ok bool
	if path[index] == MultipleTokenWildcard {
		// "*"不做名称匹配优先单匹配"?"
		n, ok = node.children[SingleTokenWildcard]
	} else {
		n, ok = node.children[path[index]]
	}
	if !ok {
		//检查单匹配
		n, ok = node.children[SingleTokenWildcard]
		if !ok {
			//检查全匹配
			n, ok = node.children[MultipleTokenWildcard]
			if !ok {
				return nil
			} else {
				return n
			}
		}
	}
	if index == len(path)-1 {
		return n
	}
	index += 1
	return findNode(n, path, index)
}
