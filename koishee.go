package main

import (
	"github.com/coreos/go-etcd/etcd"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"encoding/json"
	"os/exec"
	"strconv"
)

var etcdUrl = flag.String("etcd", "http://127.0.0.1:2379", "etcd endpoint")
var skydnsDomain = flag.String("domain", "skydns.local", "skydns domain")
var skydnsLocal = flag.String("local", unpanic(os.Hostname()).(string) + ".nodes.skydns.local", "skydns local part")

var localPrefix, globalPrefix string

func unpanic(first interface{}, err error) interface{} {
	if err != nil {
		panic(err)
	}
	return first
}

func main() {
	flag.Usage = func() {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		fmt.Printf("  %s [options]\n\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	globalPrefix = preparePrefix(*skydnsDomain)
	localPrefix = preparePrefix(*skydnsLocal)

	etcdApi := etcd.NewClient([]string{*etcdUrl})

	boot(etcdApi, globalPrefix)
	boot(etcdApi, localPrefix)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { observe(etcdApi, globalPrefix); wg.Done() }()
	go func() { observe(etcdApi, localPrefix); wg.Done() }()
	wg.Wait()
}
func boot(etcdApi *etcd.Client, prefix string) {
	resp, err := etcdApi.Get(prefix, false, true)
	if err != nil {
		return
	}
	process(resp.Node, resp.Action)
}

func observe(etcdApi *etcd.Client, prefix string) {
	ch := make(chan *etcd.Response)
	go etcdApi.Watch(prefix, 0, true, ch, nil)
	for {
		select {
		case response := <-ch:
			process(response.Node, response.Action)
		}
	}
}

type record struct {
	Host string `json:"host,omitempty"`
	Port int `json:"port,omitempty"`
	Proto string `json:"proto,omitempty"`
	Labels map[string]string `json:"labels,omitempty"`
}

func process(node *etcd.Node, action string) {
	switch action {
	case "set":
		var rec record
		err := json.Unmarshal([]byte(node.Value), &rec)
		if err != nil {
			panic(err)
		}
		parts := strings.Split(node.Key, "/")
		name := parts[len(parts)-4]
		cmd := exec.Command(flag.Args()[0], flag.Args()[1:]...)
		cmd.Env = []string{
			"svc_action=start",
			"svc_name="+name,
			"svc_host="+rec.Host,
			"svc_port="+strconv.Itoa(rec.Port),
			"svc_proto="+rec.Proto}
		for key, value := range rec.Labels {
			cmd.Env = append(cmd.Env, "svc_attr_" + key + "=" + value)
		}
		cmd.Run()
	case "delete":
		parts := strings.Split(node.Key, "/")
		name := parts[len(parts)-1]
		cmd := exec.Command(flag.Args()[0], flag.Args()[1:]...)
		cmd.Env = []string{"svc_action=stop", "svc_name="+name}
		cmd.Run()
	}
}

func preparePrefix(unprepared string) string {
	split := strings.Split(unprepared, ".")
	for i, j := 0, len(split)-1; i < j; i, j = i+1, j-1 {
		split[i], split[j] = split[j], split[i]
	}
	return "/skydns/" + strings.Join(split, "/")
}