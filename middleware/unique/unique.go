package unique

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/charm-jp/work"
	"github.com/go-redis/redis"
)

// Func defines job uniqueness.
type EnqueueCheck func(*work.Job, *work.EnqueueOptions) ([]byte, time.Duration, error)
type HandleCheck func(*work.Job, *work.DequeueOptions) ([]byte, error)

// EnqueuerOptions defines job unique key generation.
type EnqueuerOptions struct {
	Client redis.UniversalClient
	// If returned []byte is nil, uniqness check is bypassed.
	// Returned time.Duration controls how long the unique key exists.
	UniqueFunc EnqueueCheck
}

var (
	ErrDedupDuration = errors.New("work: unique duration should be > 0")
)

// Enqueuer uses UniqueFunc to ensure job uniqueness in a period.
func Enqueuer(eopt *EnqueuerOptions) work.EnqueueMiddleware {
	return func(f work.EnqueueFunc) work.EnqueueFunc {
		return func(job *work.Job, opt *work.EnqueueOptions) error {
			b, expireIn, err := eopt.UniqueFunc(job, opt)
			if err != nil {
				return err
			}
			if b == nil {
				return f(job, opt)
			}
			if expireIn <= 0 {
				return ErrDedupDuration
			}

			h := sha256.New()
			_, err = h.Write(b)
			if err != nil {
				return err
			}
			key := fmt.Sprintf("%s:unique:%s:%x", opt.Namespace, opt.QueueID, h.Sum(nil))
			notExist, err := eopt.Client.SetNX(key, 1, expireIn).Result()
			if err != nil {
				return err
			}
			if notExist {
				return f(job, opt)
			}
			return nil
		}
	}
}

type HandlerOptions struct {
	Client     redis.UniversalClient
	UniqueFunc HandleCheck
}

func HandleFunc(f work.HandleFunc, hopt HandlerOptions) work.HandleFunc {
	return func(job *work.Job, opt *work.DequeueOptions) error {
		err := f(job, opt)

		if err != nil {
			return err
		}

		b, err := hopt.UniqueFunc(job, opt)
		if err != nil {
			return err
		}
		if b == nil {
			return nil
		}

		h := sha256.New()
		_, err = h.Write(b)

		if err != nil {
			return err
		}
		key := fmt.Sprintf("%s:unique:%s:%x", opt.Namespace, opt.QueueID, h.Sum(nil))

		hopt.Client.Del(key)

		return nil
	}
}
