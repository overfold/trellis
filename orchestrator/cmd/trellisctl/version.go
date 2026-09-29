package main

import (
	"fmt"

	"github.com/overfold/trellis/internal/version"
	"github.com/spf13/cobra"
)

func NewVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:               "version",
		Short:             "Print the trellis version",
		Args:              cobra.NoArgs,
		PersistentPreRunE: func(_ *cobra.Command, _ []string) error { return nil },
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println(version.Current())
		},
	}
}
