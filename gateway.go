package main

import (
	"time"
	"sync"
	"github.com/coreos/etcd/clientv3"
	"os"
	"golang.org/x/net/context"
	"strings"
	"fmt"
	"github.com/coreos/etcd/mvcc/mvccpb"

)

type ServiceDiscover struct {
	dir string
	mutex sync.Mutex
	nodes map[string]string
}

func NewServiceDiscover(dir string) (discover *ServiceDiscover) {
	discover = 	&ServiceDiscover{
		dir: strings.TrimRight(dir, "/") + "/",
		nodes: make(map[string]string),
	}
	return
}

func (discover *ServiceDiscover)watch() {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		os.Exit(1)
	}

	var curRevision int64 = 0

	// 先读当前所有孩子, 直到成功为止
	kv := clientv3.NewKV(client)
	for {
		rangeResp, err := kv.Get(context.TODO(), discover.dir, clientv3.WithPrefix())
		if err != nil {
			continue
		}

		discover.mutex.Lock()
		for _, kv := range rangeResp.Kvs {
			discover.nodes[string(kv.Key)] = string(kv.Value)
		}
		discover.mutex.Unlock()

		// 从当前版本开始订阅
		curRevision = rangeResp.Header.Revision + 1
		break
	}

	// 监听后续的PUT与DELETE事件
	watcher := clientv3.NewWatcher(client)
	watchChan := watcher.Watch(context.TODO(), discover.dir, clientv3.WithPrefix(), clientv3.WithRev(curRevision))
	for watchResp := range watchChan {
		for _, event := range watchResp.Events {
			discover.mutex.Lock()
			switch (event.Type) {
			case mvccpb.PUT:
				fmt.Println("PUT事件")
				discover.nodes[string(event.Kv.Key)] = string(event.Kv.Value)
			case mvccpb.DELETE:
				delete(discover.nodes, string(event.Kv.Key))
				fmt.Println("DELETE事件")
			}
			discover.mutex.Unlock()
		}
	}
}

func (discover *ServiceDiscover)Nodes() (nodes []string) {
	dupNodes := map[string]bool{}
	nodes = []string{}

	// 按endpoint去重
	discover.mutex.Lock()
	for _, endpoint := range discover.nodes {
		dupNodes[endpoint] = true
	}
	discover.mutex.Unlock()

	for endpoint, _ := range dupNodes {
		nodes = append(nodes, endpoint)
	}
	return
}

func main()  {
	discover := NewServiceDiscover("/agent")
	go discover.watch()

	for {
		time.Sleep(time.Duration(1) * time.Second)
		fmt.Println(discover.Nodes())
	}
}