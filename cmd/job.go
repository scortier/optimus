package cmd

import (
	"github.com/odpf/optimus/config"

	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	cli "github.com/spf13/cobra"
)

func jobCommand(l log.Logger, jobSpecFs afero.Fs, jobSpecRepo JobSpecRepository,
	pluginRepo models.PluginRepository, conf config.Provider) *cli.Command {
	cmd := &cli.Command{
		Use:   "job",
		Short: "Interact with schedulable Job",
		Annotations: map[string]string{
			"group:core": "true",
		},
	}
	if jobSpecRepo != nil {
		cmd.AddCommand(jobCreateCommand(l, jobSpecFs, jobSpecRepo, pluginRepo))
		cmd.AddCommand(jobAddHookCommand(l, jobSpecRepo, pluginRepo))
		cmd.AddCommand(jobRenderTemplateCommand(l, jobSpecRepo))
		cmd.AddCommand(jobValidateCommand(l, pluginRepo, jobSpecRepo, conf))
		cmd.AddCommand(jobRunCommand(l, jobSpecRepo, pluginRepo, conf))
	}
	cmd.AddCommand(jobStatusCommand(l, conf))
	return cmd
}
