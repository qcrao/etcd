// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package auth

import (
	"github.com/coreos/etcd/auth/authpb"
	"github.com/coreos/etcd/mvcc/backend"
	"github.com/coreos/etcd/pkg/adt"
)

// 通过username获取聚合后的用户权限信息
func getMergedPerms(tx backend.BatchTx, userName string) *unifiedRangePermissions {
	user := getUser(tx, userName)
	if user == nil {
		plog.Errorf("invalid user name %s", userName)
		return nil
	}

	// 读写线段树
	readPerms := &adt.IntervalTree{}
	writePerms := &adt.IntervalTree{}

	for _, roleName := range user.Roles {
		// 通过角色名获取角色信息
		role := getRole(tx, roleName)
		if role == nil {
			continue
		}

		for _, perm := range role.KeyPermission {
			var ivl adt.Interval
			var rangeEnd []byte

			// 如果end有效。
			if len(perm.RangeEnd) != 1 || perm.RangeEnd[0] != 0 {
				rangeEnd = perm.RangeEnd
			}

			// 通过上面的判断可以过滤掉perm.RangeEnd数组长度为1的，且byte[0]==0
			if len(perm.RangeEnd) != 0 {
				ivl = adt.NewBytesAffineInterval(perm.Key, rangeEnd)
			} else {
				ivl = adt.NewBytesAffinePoint(perm.Key)
			}

			switch perm.PermType {
			case authpb.READWRITE:
				readPerms.Insert(ivl, struct{}{})
				writePerms.Insert(ivl, struct{}{})

			case authpb.READ:
				readPerms.Insert(ivl, struct{}{})

			case authpb.WRITE:
				writePerms.Insert(ivl, struct{}{})
			}
		}
	}

	return &unifiedRangePermissions{
		readPerms:  readPerms,
		writePerms: writePerms,
	}
}

// 检查给定范围的key是否有权限
func checkKeyInterval(cachedPerms *unifiedRangePermissions, key, rangeEnd []byte, permtyp authpb.Permission_Type) bool {
	if len(rangeEnd) == 1 && rangeEnd[0] == 0 {
		rangeEnd = nil
	}

	ivl := adt.NewBytesAffineInterval(key, rangeEnd)
	switch permtyp {
	case authpb.READ:
		return cachedPerms.readPerms.Contains(ivl)
	case authpb.WRITE:
		return cachedPerms.writePerms.Contains(ivl)
	default:
		plog.Panicf("unknown auth type: %v", permtyp)
	}
	return false
}

// 检查给定key是否有权限
func checkKeyPoint(cachedPerms *unifiedRangePermissions, key []byte, permtyp authpb.Permission_Type) bool {
	pt := adt.NewBytesAffinePoint(key)
	switch permtyp {
	case authpb.READ:
		return cachedPerms.readPerms.Intersects(pt)
	case authpb.WRITE:
		return cachedPerms.writePerms.Intersects(pt)
	default:
		plog.Panicf("unknown auth type: %v", permtyp)
	}
	return false
}

// 检查给定用户是否有给定范围的key权限
// 先通过用户名获取到所有权限，然后check权限
func (as *authStore) isRangeOpPermitted(tx backend.BatchTx, userName string, key, rangeEnd []byte, permtyp authpb.Permission_Type) bool {
	// assumption: tx is Lock()ed
	_, ok := as.rangePermCache[userName]
	if !ok {
		perms := getMergedPerms(tx, userName)
		if perms == nil {
			plog.Errorf("failed to create a unified permission of user %s", userName)
			return false
		}
		as.rangePermCache[userName] = perms
	}

	if len(rangeEnd) == 0 {
		return checkKeyPoint(as.rangePermCache[userName], key, permtyp)
	}

	return checkKeyInterval(as.rangePermCache[userName], key, rangeEnd, permtyp)
}

// 清空权限池
func (as *authStore) clearCachedPerm() {
	as.rangePermCache = make(map[string]*unifiedRangePermissions)
}

// 删掉给定用户的权限
func (as *authStore) invalidateCachedPerm(userName string) {
	delete(as.rangePermCache, userName)
}

type unifiedRangePermissions struct {
	readPerms  *adt.IntervalTree
	writePerms *adt.IntervalTree
}
