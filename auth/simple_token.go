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

// CAUTION: This randum number based token mechanism is only for testing purpose.
// JWT based mechanism will be added in the near future.

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	letters                  = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	defaultSimpleTokenLength = 16
)

// var for testing purposes
var (
	simpleTokenTTL           = 5 * time.Minute
	simpleTokenTTLResolution = 1 * time.Second
)

// token TTL 维护者
type simpleTokenTTLKeeper struct {
	tokens          map[string]time.Time
	donec           chan struct{}
	stopc           chan struct{}
	deleteTokenFunc func(string)
	mu              *sync.Mutex
}

// 往stopc中塞入空结构体，等待donec ch
func (tm *simpleTokenTTLKeeper) stop() {
	select {
	case tm.stopc <- struct{}{}:
	case <-tm.donec:
	}
	<-tm.donec
}

// 添加一个token，过期时间为当前时间加上5分钟
func (tm *simpleTokenTTLKeeper) addSimpleToken(token string) {
	tm.tokens[token] = time.Now().Add(simpleTokenTTL)
}

// 重置token的ttl为5分钟
func (tm *simpleTokenTTLKeeper) resetSimpleToken(token string) {
	if _, ok := tm.tokens[token]; ok {
		tm.tokens[token] = time.Now().Add(simpleTokenTTL)
	}
}

// 删除token
func (tm *simpleTokenTTLKeeper) deleteSimpleToken(token string) {
	delete(tm.tokens, token)
}

// 每一秒钟，遍历token，删除过期的token
func (tm *simpleTokenTTLKeeper) run() {
	tokenTicker := time.NewTicker(simpleTokenTTLResolution)
	defer func() {
		tokenTicker.Stop()
		close(tm.donec)
	}()
	for {
		select {
		case <-tokenTicker.C:
			nowtime := time.Now()
			tm.mu.Lock()
			for t, tokenendtime := range tm.tokens {
				if nowtime.After(tokenendtime) {
					tm.deleteTokenFunc(t) // 删掉上层的username->token
					delete(tm.tokens, t)
				}
			}
			tm.mu.Unlock()
		case <-tm.stopc:
			return
		}
	}
}

// token维护者
type tokenSimple struct {
	indexWaiter       func(uint64) <-chan struct{}
	simpleTokenKeeper *simpleTokenTTLKeeper
	simpleTokensMu    sync.Mutex
	simpleTokens      map[string]string // token -> username
}

// 生成token的前缀，16位字符
func (t *tokenSimple) genTokenPrefix() (string, error) {
	ret := make([]byte, defaultSimpleTokenLength)

	for i := 0; i < defaultSimpleTokenLength; i++ {
		bInt, err := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		if err != nil {
			return "", err
		}

		ret[i] = letters[bInt.Int64()]
	}

	return string(ret), nil
}

// 将指定token授权给指定username
// 如果指定的token已经存在，则panic
func (t *tokenSimple) assignSimpleTokenToUser(username, token string) {
	t.simpleTokensMu.Lock()
	defer t.simpleTokensMu.Unlock()
	if t.simpleTokenKeeper == nil {
		return
	}

	_, ok := t.simpleTokens[token]
	if ok {
		plog.Panicf("token %s is alredy used", token)
	}

	t.simpleTokens[token] = username
	t.simpleTokenKeeper.addSimpleToken(token)
}

// 删掉username对应的token
func (t *tokenSimple) invalidateUser(username string) {
	if t.simpleTokenKeeper == nil {
		return
	}
	t.simpleTokensMu.Lock()
	for token, name := range t.simpleTokens {
		if strings.Compare(name, username) == 0 {
			delete(t.simpleTokens, token)
			t.simpleTokenKeeper.deleteSimpleToken(token)
		}
	}
	t.simpleTokensMu.Unlock()
}

// 启动
func (t *tokenSimple) enable() {
	delf := func(tk string) {
		if username, ok := t.simpleTokens[tk]; ok {
			plog.Infof("deleting token %s for user %s", tk, username)
			delete(t.simpleTokens, tk)
		}
	}
	t.simpleTokenKeeper = &simpleTokenTTLKeeper{
		tokens:          make(map[string]time.Time),
		donec:           make(chan struct{}),
		stopc:           make(chan struct{}),
		deleteTokenFunc: delf,
		mu:              &t.simpleTokensMu,
	}
	go t.simpleTokenKeeper.run()
}

// 清空
func (t *tokenSimple) disable() {
	t.simpleTokensMu.Lock()
	tk := t.simpleTokenKeeper
	t.simpleTokenKeeper = nil
	t.simpleTokens = make(map[string]string) // invalidate all tokens
	t.simpleTokensMu.Unlock()
	if tk != nil {
		tk.stop()
	}
}

// 验证token，且会重置token
func (t *tokenSimple) info(ctx context.Context, token string, revision uint64) (*AuthInfo, bool) {
	if !t.isValidSimpleToken(ctx, token) {
		return nil, false
	}
	t.simpleTokensMu.Lock()
	username, ok := t.simpleTokens[token]
	if ok && t.simpleTokenKeeper != nil {
		t.simpleTokenKeeper.resetSimpleToken(token)
	}
	t.simpleTokensMu.Unlock()
	return &AuthInfo{Username: username, Revision: revision}, ok
}

// 赋予user token
func (t *tokenSimple) assign(ctx context.Context, username string, rev uint64) (string, error) {
	// rev isn't used in simple token, it is only used in JWT
	index := ctx.Value(AuthenticateParamIndex{}).(uint64)
	simpleTokenPrefix := ctx.Value(AuthenticateParamSimpleTokenPrefix{}).(string)
	token := fmt.Sprintf("%s.%d", simpleTokenPrefix, index)
	t.assignSimpleTokenToUser(username, token)

	return token, nil
}

// 判定token是否合法
func (t *tokenSimple) isValidSimpleToken(ctx context.Context, token string) bool {
	splitted := strings.Split(token, ".")
	if len(splitted) != 2 {
		return false
	}
	index, err := strconv.Atoi(splitted[1])
	if err != nil {
		return false
	}

	select {
	case <-t.indexWaiter(uint64(index)):
		return true
	case <-ctx.Done():
	}

	return false
}

func newTokenProviderSimple(indexWaiter func(uint64) <-chan struct{}) *tokenSimple {
	return &tokenSimple{
		simpleTokens: make(map[string]string),
		indexWaiter:  indexWaiter,
	}
}
