package main

import (
	"testing"

	"github.com/bubustack/bubu-sdk-go/conformance"
	"github.com/bubustack/map-reduce-adapter-engram/pkg/config"
	"github.com/bubustack/map-reduce-adapter-engram/pkg/engram"
)

func TestConformance(t *testing.T) {
	suite := conformance.BatchSuite[config.Config, config.Inputs]{
		Engram:      engram.New(),
		Config:      config.Config{},
		Inputs:      config.Inputs{},
		ExpectError: true,
		ValidateError: func(err error) error {
			if err == nil {
				return nil
			}
			if err.Error() != "map.storyRef or map.story is required" {
				return err
			}
			return nil
		},
	}
	suite.Run(t)
}
