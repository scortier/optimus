package local_test

import (
	"context"
	"testing"

	"github.com/odpf/optimus/models"

	"github.com/odpf/optimus/mock"

	"github.com/odpf/optimus/store/local"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

func TestSpecAdapter(t *testing.T) {
	t.Run("should convert job with task from yaml to optimus model & back successfully", func(t *testing.T) {
		yamlSpec := `
version: 1
name: test_job
owner: test@example.com
schedule:
  start_date: "2021-02-03"
  interval: 0 2 * * *
behavior:
  depends_on_past: true
  catch_up: false
task:
  name: bq2bq
  config:
    PROJECT: project
    DATASET: dataset
    TABLE: table
    SQL_TYPE: STANDARD
    LOAD_METHOD: REPLACE
  window:
    size: 168h
    offset: 0
    truncate_to: w
labels:
  orchestrator: optimus
dependencies: []
hooks: []
`
		var localJobParsed local.Job
		err := yaml.Unmarshal([]byte(yamlSpec), &localJobParsed)
		assert.Nil(t, err)

		bq2bqTrasnformer := new(mock.TaskPlugin)
		bq2bqTrasnformer.On("GetTaskSchema", context.Background(), models.GetTaskSchemaRequest{}).Return(models.GetTaskSchemaResponse{
			Name: "bq2bq",
		}, nil)

		allTasksRepo := new(mock.SupportedTaskRepo)
		allTasksRepo.On("GetByName", "bq2bq").Return(bq2bqTrasnformer, nil)

		adapter := local.NewJobSpecAdapter(allTasksRepo, nil)
		modelJob, err := adapter.ToSpec(localJobParsed)
		assert.Nil(t, err)

		localJobBack, err := adapter.FromSpec(modelJob)
		assert.Nil(t, err)

		assert.Equal(t, localJobParsed, localJobBack)
	})
}

func TestJob_MergeFrom(t *testing.T) {
	type fields struct {
		child    local.Job
		expected local.Job
	}
	type args struct {
		parent local.Job
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "should successfully copy version if child has zero value",
			fields: fields{
				child: local.Job{
					Version: 0,
				},
				expected: local.Job{
					Version: 1,
				},
			},
			args: args{
				parent: local.Job{
					Version: 1,
				},
			},
		},
		{
			name: "should successfully copy root level values if child has zero value",
			fields: fields{
				child: local.Job{},
				expected: local.Job{
					Description: "hey",
					Labels: map[string]string{
						"optimus": "prime",
						"gogo":    "gadget",
					},
					Behavior: local.JobBehavior{
						DependsOnPast: false,
						Catchup:       true,
						Retry: local.JobBehaviorRetry{
							Count:              3,
							Delay:              "2m",
							ExponentialBackoff: false,
						},
					},
					Schedule: local.JobSchedule{
						StartDate: "2020",
						EndDate:   "2021",
						Interval:  "@daily",
					},
				},
			},
			args: args{
				parent: local.Job{
					Description: "hey",
					Labels: map[string]string{
						"optimus": "prime",
						"gogo":    "gadget",
					},
					Behavior: local.JobBehavior{
						DependsOnPast: false,
						Catchup:       true,
						Retry: local.JobBehaviorRetry{
							Count:              3,
							Delay:              "2m",
							ExponentialBackoff: false,
						},
					},
					Schedule: local.JobSchedule{
						StartDate: "2020",
						EndDate:   "2021",
						Interval:  "@daily",
					},
				},
			},
		},
		{
			name: "should not merge if child already contains non zero values",
			fields: fields{
				child: local.Job{
					Version: 2,
				},
				expected: local.Job{
					Version: 2,
				},
			},
			args: args{
				parent: local.Job{
					Version: 1,
				},
			},
		},
		{
			name: "should merge task configs properly",
			fields: fields{
				child: local.Job{
					Task: local.JobTask{
						Name: "panda",
						Config: []yaml.MapItem{
							{
								Key:   "dance",
								Value: "happy",
							},
						},
					},
				},
				expected: local.Job{
					Task: local.JobTask{
						Name: "panda",
						Config: []yaml.MapItem{
							{
								Key:   "dance",
								Value: "happy",
							},
							{
								Key:   "eat",
								Value: "ramen",
							},
						},
					},
				},
			},
			args: args{
				parent: local.Job{
					Task: local.JobTask{
						Name: "panda",
						Config: []yaml.MapItem{
							{
								Key:   "eat",
								Value: "ramen",
							},
						},
					},
				},
			},
		},
		{
			name: "should merge hooks configs properly",
			fields: fields{
				child: local.Job{
					Hooks: []local.JobHook{
						{
							Name: "kungfu",
						},
						{
							Name: "martial",
							Config: []yaml.MapItem{
								{
									Key:   "arts",
									Value: "2",
								},
							},
						},
					},
				},
				expected: local.Job{
					Hooks: []local.JobHook{
						{
							Name: "kungfu",
							Config: []yaml.MapItem{
								{
									Key:   "ninza",
									Value: "run",
								},
							},
						},
						{
							Name: "martial",
							Config: []yaml.MapItem{
								{
									Key:   "arts",
									Value: "2",
								},
								{
									Key:   "kick",
									Value: "high",
								},
							},
						},
						{
							Name: "saitama",
							Config: []yaml.MapItem{
								{
									Key:   "punch",
									Value: 1,
								},
							},
						},
					},
				},
			},
			args: args{
				parent: local.Job{
					Hooks: []local.JobHook{
						{
							Name: "kungfu",
							Config: []yaml.MapItem{
								{
									Key:   "ninza",
									Value: "run",
								},
							},
						},
						{
							Name: "martial",
							Config: []yaml.MapItem{
								{
									Key:   "arts",
									Value: "3",
								},
								{
									Key:   "kick",
									Value: "high",
								},
							},
						},
						{
							Name: "saitama",
							Config: []yaml.MapItem{
								{
									Key:   "punch",
									Value: 1,
								},
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fields.child.MergeFrom(tt.args.parent)
			assert.Equal(t, tt.fields.expected.Version, tt.fields.child.Version)
			assert.Equal(t, tt.fields.expected.Name, tt.fields.child.Name)
			assert.Equal(t, tt.fields.expected.Description, tt.fields.child.Description)
			assert.Equal(t, tt.fields.expected.Schedule.Interval, tt.fields.child.Schedule.Interval)
			assert.Equal(t, tt.fields.expected.Schedule.StartDate, tt.fields.child.Schedule.StartDate)
			assert.Equal(t, tt.fields.expected.Schedule.EndDate, tt.fields.child.Schedule.EndDate)
			assert.Equal(t, tt.fields.expected.Behavior.DependsOnPast, tt.fields.child.Behavior.DependsOnPast)
			assert.Equal(t, tt.fields.expected.Behavior.Catchup, tt.fields.child.Behavior.Catchup)
			assert.Equal(t, tt.fields.expected.Behavior.Retry.Count, tt.fields.child.Behavior.Retry.Count)
			assert.Equal(t, tt.fields.expected.Behavior.Retry.Delay, tt.fields.child.Behavior.Retry.Delay)
			assert.Equal(t, tt.fields.expected.Behavior.Retry.ExponentialBackoff, tt.fields.child.Behavior.Retry.ExponentialBackoff)
			assert.ElementsMatch(t, tt.fields.expected.Dependencies, tt.fields.child.Dependencies)
			assert.Equal(t, len(tt.fields.expected.Labels), len(tt.fields.child.Labels))
			assert.Equal(t, tt.fields.expected.Task.Name, tt.fields.child.Task.Name)
			assert.Equal(t, tt.fields.expected.Task.Window.Offset, tt.fields.child.Task.Window.Offset)
			assert.Equal(t, tt.fields.expected.Task.Window.Size, tt.fields.child.Task.Window.Size)
			assert.Equal(t, tt.fields.expected.Task.Window.TruncateTo, tt.fields.child.Task.Window.TruncateTo)
			assert.ElementsMatch(t, tt.fields.expected.Task.Config, tt.fields.child.Task.Config)
			for idx, eh := range tt.fields.expected.Hooks {
				assert.Equal(t, eh.Name, tt.fields.child.Hooks[idx].Name)
				assert.ElementsMatch(t, eh.Config, tt.fields.child.Hooks[idx].Config)
			}
		})
	}
}
