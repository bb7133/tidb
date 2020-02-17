// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package collate

import (
	"strings"
)

var (
	collatorMap         map[string]Collator
	collatorIDMap       map[int]Collator
	newCollationEnabled bool
)

// CollatorOption is the option of collator.
type CollatorOption struct {
	PadLen int
}

// Collator provides functionality for comparing strings for a given
// collation order.
type Collator interface {
	// Compare returns an integer comparing the two strings. The result will be 0 if a == b, -1 if a < b, and +1 if a > b.
	Compare(a, b string, opt CollatorOption) int
	// Key returns the collate key for str. If the collation is padding, make sure the PadLen >= len(rune[]str) in opt.
	Key(str string, opt CollatorOption) []byte
}

// SetNewCollationEnabled sets if the new collation are enabled.
// Note: Be careful to use this function, if this functions is used in tests, make sure the tests are serial.
func SetNewCollationEnabled(flag bool) {
	newCollationEnabled = flag
	if newCollationEnabled {
		collatorMap["utf8mb4_bin"] = &binPaddingCollator{}
		collatorMap["utf8_bin"] = &binPaddingCollator{}
		collatorMap["utf8mb4_general_ci"] = &generalCICollator{}
		collatorMap["utf8_general_ci"] = &generalCICollator{}

		collatorIDMap[46] = &binPaddingCollator{}
		collatorIDMap[83] = &binPaddingCollator{}
		collatorIDMap[45] = &generalCICollator{}
		collatorIDMap[33] = &generalCICollator{}
	} else {
		collatorMap["utf8mb4_bin"] = &binCollator{}
		collatorMap["utf8_bin"] = &binCollator{}
		collatorMap["utf8mb4_general_ci"] = &binCollator{}
		collatorMap["utf8_general_ci"] = &binCollator{}

		collatorIDMap[46] = &binCollator{}
		collatorIDMap[83] = &binCollator{}
		collatorIDMap[45] = &binCollator{}
		collatorIDMap[33] = &binCollator{}
	}
}

// NewCollationEnabled returns if the new collations are enabled.
func NewCollationEnabled() bool {
	return newCollationEnabled
}

// GetCollator get the collator according to collate, it will return the binary collator if the corresponding collator doesn't exist.
func GetCollator(collate string) Collator {
	ctor, ok := collatorMap[collate]
	if !ok {
		return collatorMap["utf8mb4_bin"]
	}
	return ctor
}

// GetCollatorByID get the collator according to id, it will return the binary collator if the corresponding collator doesn't exist.
func GetCollatorByID(id int) Collator {
	ctor, ok := collatorIDMap[id]
	if !ok {
		return collatorMap["utf8mb4_bin"]
	}
	return ctor
}

type binCollator struct {
}

// Compare implement Collator interface.
func (bc *binCollator) Compare(a, b string, opt CollatorOption) int {
	return strings.Compare(a, b)
}

// Key implement Collator interface.
func (bc *binCollator) Key(str string, opt CollatorOption) []byte {
	return []byte(str)
}

type binPaddingCollator struct {
}

func (bpc *binPaddingCollator) Compare(a, b string, opt CollatorOption) int {
	aLen := len(a)
	bLen := len(b)
	if aLen > bLen {
		return strings.Compare(a, b+strings.Repeat(" ", aLen-bLen))
	} else if aLen < bLen {
		return strings.Compare(a+strings.Repeat(" ", bLen-aLen), b)
	}
	return strings.Compare(a, b)
}

func (bpc *binPaddingCollator) Key(str string, opt CollatorOption) []byte {
	return []byte(str + strings.Repeat(" ", opt.PadLen-len(str)))
}

func init() {
	collatorMap = make(map[string]Collator)
	collatorIDMap = make(map[int]Collator)

	collatorMap["binary"] = &binCollator{}
	collatorMap["utf8mb4_bin"] = &binCollator{}
	collatorMap["utf8_bin"] = &binCollator{}
	collatorMap["utf8mb4_general_ci"] = &binCollator{}
	collatorMap["utf8_general_ci"] = &binCollator{}

	// See https://github.com/pingcap/parser/blob/master/charset/charset.go for more information about the IDs.
	collatorIDMap[63] = &binCollator{}
	collatorIDMap[46] = &binCollator{}
	collatorIDMap[83] = &binCollator{}
	collatorIDMap[45] = &binCollator{}
	collatorIDMap[33] = &binCollator{}
}
