package cmd

import (
    "github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
    Use:        "Parquet CLI",
    Short:      "Parquet CLI for reading parquet files",
}

func Execute() error {
    return rootCmd.Execute()
}

