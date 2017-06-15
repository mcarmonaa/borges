package main

import (
	"fmt"
	"os"

	"github.com/src-d/borges"

	"srcd.works/core.v0"
	"srcd.works/framework.v0/queue"
	"gopkg.in/src-d/go-git.v4/utils/ioutil"
)

const (
	producerCmdName      = "producer"
	producerCmdShortDesc = "create new jobs and put them into the queue"
	producerCmdLongDesc  = ""
)

type producerCmd struct {
	cmd
	Source        string `long:"source" default:"mentions" description:"source to produce jobs from (mentions, file)"`
	MentionsQueue string `long:"mentionsqueue" default:"rovers" description:"queue name used to obtain mentions if the source type is 'mentions'"`
	File          string `long:"file" description:"path to a file to read URLs from, used with --source=file"`
}

func (c *producerCmd) Execute(args []string) error {
	b, err := queue.NewBroker(c.Broker)
	if err != nil {
		return err
	}

	defer b.Close()
	q, err := b.Queue(c.Queue)
	if err != nil {
		return err
	}

	ji, err := c.jobIter(b)
	if err != nil {
		return err
	}
	defer ioutil.CheckClose(ji, &err)

	p := borges.NewProducer(ji, q)
	p.Notifiers.Done = c.notifier
	p.Start()
	return err
}

func (c *producerCmd) jobIter(b queue.Broker) (borges.JobIter, error) {
	storer := core.ModelRepositoryStore()

	switch c.Source {
	case "mentions":
		q, err := b.Queue(c.MentionsQueue)
		if err != nil {
			return nil, err
		}
		return borges.NewMentionJobIter(q, storer), nil
	case "file":
		f, err := os.Open(c.File)
		if err != nil {
			return nil, err
		}
		return borges.NewLineJobIter(f, storer), nil
	default:
		return nil, fmt.Errorf("invalid source: %s", c.Source)
	}
}

func (c *producerCmd) notifier(j *borges.Job, err error) {
	if err != nil {
		logger.Error("job queue error", "RepositoryID", j.RepositoryID, "error", err)
	} else {
		logger.Info("job queued", "RepositoryID", j.RepositoryID)
	}
}