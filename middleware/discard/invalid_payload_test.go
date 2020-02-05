package discard

import (
	"testing"

	"github.com/charm-jp/work"
	"github.com/stretchr/testify/require"
)

func TestInvalidPayload(t *testing.T) {
	job := work.NewJob()
	opt := &work.DequeueOptions{
		Namespace: "n1",
		QueueID:   "q1",
	}
	h := InvalidPayload(func(*work.Job, *work.DequeueOptions) error {
		var s string
		return job.UnmarshalPayload(&s)
	})

	err := h(job, opt)
	require.Error(t, err)
	require.Equal(t, work.ErrUnrecoverable, err)
}
