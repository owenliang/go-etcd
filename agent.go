package main

import (
	"time"
	"github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
	"strings"
	"fmt"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"os"
)

/**
	目录结构:
	/serviceName/leaseId -> endpoint
 */
func Register(dir string, value string) {
	dir = strings.TrimRight(dir, "/") + "/"

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		os.Exit(1)
	}

	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)
	var curLeaseId clientv3.LeaseID = 0

	for {
		if curLeaseId == 0 {
			leaseResp, err := lease.Grant(context.TODO(), 10)
			if err != nil {
				goto SLEEP
			}

			key := dir + fmt.Sprintf("%d", leaseResp.ID)
			if _, err := kv.Put(context.TODO(), key, value, clientv3.WithLease(leaseResp.ID)); err != nil {
				goto SLEEP
			}
			curLeaseId = leaseResp.ID
		} else {
			fmt.Printf("keepalive curLeaseId=%d\n", curLeaseId)
			if _, err := lease.KeepAliveOnce(context.TODO(), curLeaseId); err == rpctypes.ErrLeaseNotFound {
				curLeaseId = 0
				continue
			}
		}
	SLEEP:
		time.Sleep(time.Duration(1) * time.Second)
	}
}

func main()  {
	go Register("/agent", "192.168.1.2:80")

	for {
		time.Sleep(time.Duration(1) * time.Second)
	}
}