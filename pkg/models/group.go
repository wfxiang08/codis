// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

const MaxGroupId = 9999

type Group struct {
	Id      int            `json:"id"`
	// 每一个Group可以包含 GroupServer
	Servers []*GroupServer `json:"servers"`

	Promoting struct {
		Index int    `json:"index,omitempty"`
		State string `json:"state,omitempty"`
	} `json:"promoting"`

	// 失去同步? 如何处理呢?
	OutOfSync bool `json:"out_of_sync"`
}

type GroupServer struct {
	Addr       string `json:"server"`
	DataCenter string `json:"datacenter"`

	Action struct {
		Index int    `json:"index,omitempty"`
		State string `json:"state,omitempty"`
	} `json:"action"`

	ReplicaGroup bool `json:"replica_group"`
}

func (g *Group) Encode() []byte {
	return jsonEncode(g)
}
