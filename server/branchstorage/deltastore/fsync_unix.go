// Copyright 2026 Dolthub, Inc.
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

//go:build unix

package deltastore

import (
	"errors"
	"syscall"
)

// isInvalidDirSync detects EINVAL from a directory fsync. Some filesystems
// (notably tmpfs on certain Linux configs) return EINVAL when asked to fsync
// a directory; the documented best practice is to ignore.
func isInvalidDirSync(err error) bool {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno == syscall.EINVAL
	}
	return false
}
