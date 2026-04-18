package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

const Unlocked = ""

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockKey string
	version rpc.Tversion
	id      string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, lockKey: lockname, id: kvtest.RandValue(8)}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	//fmt.Println(lk.id, "is acquiring lock")
	for {
		val, ver, _ := lk.ck.Get(lk.lockKey) // err doesn't matter because if err, ver will be ""
		if val == Unlocked {
			err := lk.ck.Put(lk.lockKey, lk.id, ver)
			if err == rpc.OK {
				//fmt.Println(lk.id, "Acquired Lock from OK")
				lk.version = ver + 1
				return
			} else if err == rpc.ErrMaybe { // could have succeeded, could have failed.
				dcVal, dcVer, dcErr := lk.ck.Get(lk.lockKey)
				if dcVal == lk.id && dcVer == ver+1 && dcErr == rpc.OK { //successful PUT
					//fmt.Println(lk.id, "Acquired Lock from Maybe")
					lk.version = ver + 1
					return
				}
			}
			continue
		}
		time.Sleep(100 * time.Millisecond)
	}

}

func (lk *Lock) Release() {
	// Your code here
	for {
		ok := lk.ck.Put(lk.lockKey, Unlocked, lk.version)
		if ok == rpc.OK {
			//fmt.Println(lk.id, "Released Lock from OK")
			return
		} else if ok == rpc.ErrMaybe {
			dcVal, _, _ := lk.ck.Get(lk.lockKey)
			if dcVal != lk.id { //Release was successful
				//fmt.Println(lk.id, "Released Lock from Maybe")
				return
			}
		}
		//fmt.Println(lk.id, "releases lock, and gets", ok)
		time.Sleep(100 * time.Millisecond)
	}

}
