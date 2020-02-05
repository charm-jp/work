package discard

import (
	"errors"
	"testing"
	"time"

	"github.com/charm-jp/work"
	"github.com/stretchr/testify/require"
)

func TestAfter(t *testing.T) {
	job := work.NewJob()
	opt := &work.DequeueOptions{
		Namespace: "n1",
		QueueID:   "q1",
	}
	d := After(time.Minute)
	h := d(func(*work.Job, *work.DequeueOptions) error {
		return errors.New("no reason")
	})

	err := h(job, opt)
	require.Error(t, err)
	require.NotEqual(t, work.ErrUnrecoverable, err)

	job.CreatedAt = job.CreatedAt.Add(-time.Hour)
	err = h(job, opt)
	require.Equal(t, work.ErrUnrecoverable, err)
}
