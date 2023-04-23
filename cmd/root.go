/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"backup/helper"
	"os"

	"github.com/spf13/cobra"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "backup",
	Short: "This is a program used to dump MySql",
	Long: `This is a command-line tool that can be used to dump MySql databases and export an .sql file. 
Please modify the configuration file before running. The default configuration file is .backup.yml
    
For example:

	backup -f xx.yml
	
 `,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {

		cfg := helper.LoadCofig(cfgFile)
		cfg.Start()
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "f", "./.backup.yaml", "config file (default is ./.backup.yaml)")

}
