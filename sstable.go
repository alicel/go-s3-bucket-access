package main

import (
	"slices"
	"strings"
)

type SSTable struct {
	EntityNames       *EntityNames `json:"entityNames"`
	Size              int64        `json:"size"`
	KeyPath           string       `json:"keyPath"`
	ComponentFileKeys []string     `json:"componentFileKeys"`
}

type EntityNames struct {
	KeyspaceName      string `json:"keyspaceName"`
	CqlTableName      string `json:"cqlTableName"`
	SsTableNamePrefix string `json:"ssTableNamePrefix"`
}

func NewSSTable(entityNames *EntityNames, keyPrefix string) *SSTable {
	return &SSTable{
		EntityNames:       entityNames,
		KeyPath:           keyPrefix,
		Size:              0,
		ComponentFileKeys: []string{},
	}
}

func (sst *SSTable) addComponentFileKey(key string) {
	sst.ComponentFileKeys = append(sst.ComponentFileKeys, key)
}

func (sst *SSTable) increaseSize(deltaSize int64) {
	sst.Size = sst.Size + deltaSize
}

func (sst *SSTable) isDifferentSSTable(keyPath string, ssTableNamePrefix string) bool {
	return (sst.KeyPath != keyPath) || (sst.EntityNames.SsTableNamePrefix != ssTableNamePrefix)
}

func isSSTableComponentFile(key string) bool {
	// go by extension for now, it may be refined in the future. the naming pattern is too variable
	componentFileExtensions := []string{"CompressionInfo.db", "Data.db", "Digest.crc32", "Filter.db", "Partitions.db",
		"Rows.db", "Statistics.db", "TOC.txt"}
	fileExtensionStartIndex := strings.LastIndex(key, "-")
	return slices.Contains(componentFileExtensions, key[fileExtensionStartIndex+1:])
}
