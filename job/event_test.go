package job_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestEventService(t *testing.T) {
	log := log.NewNoop()
	eventValues, _ := structpb.NewStruct(
		map[string]interface{}{
			"url": "https://example.io",
		},
	)
	t.Run("should successfully notify registered notifiers on valid event", func(t *testing.T) {
		projectSpec := models.ProjectSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "a-data-project",
		}

		namespaceSpec := models.NamespaceSpec{
			ID:          uuid.Must(uuid.NewRandom()),
			Name:        "game_jam",
			ProjectSpec: projectSpec,
		}
		jobSpec := models.JobSpec{
			Name: "transform-tables",
			Behavior: models.JobSpecBehavior{
				Notify: []models.JobSpecNotifier{
					{
						On: models.JobEventTypeFailure,
						Channels: []string{
							"slacker://@devs",
						},
					},
				},
			},
		}
		je := models.JobEvent{
			Type:  models.JobEventTypeFailure,
			Value: eventValues.Fields,
		}

		notifier := new(mock.Notifier)
		notifier.On("Notify", context.Background(), models.NotifyAttrs{
			Namespace: namespaceSpec,
			JobSpec:   jobSpec,
			JobEvent:  je,
			Route:     "@devs",
		}).Return(nil)
		defer notifier.AssertExpectations(t)

		evtService := job.NewEventService(log, map[string]models.Notifier{
			"slacker": notifier,
		})
		err := evtService.Register(context.Background(), namespaceSpec, jobSpec, je)
		assert.Nil(t, err)
	})
	t.Run("should ignore notify events for unknown schemes", func(t *testing.T) {
		projectSpec := models.ProjectSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "a-data-project",
		}

		namespaceSpec := models.NamespaceSpec{
			ID:          uuid.Must(uuid.NewRandom()),
			Name:        "game_jam",
			ProjectSpec: projectSpec,
		}
		jobSpec := models.JobSpec{
			Name: "transform-tables",
			Behavior: models.JobSpecBehavior{
				Notify: []models.JobSpecNotifier{
					{
						On: models.JobEventTypeFailure,
						Channels: []string{
							"blocker://@devs",
						},
					},
				},
			},
		}
		je := models.JobEvent{
			Type:  models.JobEventTypeFailure,
			Value: eventValues.GetFields(),
		}

		notifier := new(mock.Notifier)
		defer notifier.AssertExpectations(t)

		evtService := job.NewEventService(log, map[string]models.Notifier{
			"slacker": notifier,
		})
		err := evtService.Register(context.Background(), namespaceSpec, jobSpec, je)
		assert.Nil(t, err)
	})
	t.Run("should fail if failed to notify registered notifiers on valid event", func(t *testing.T) {
		projectSpec := models.ProjectSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "a-data-project",
		}

		namespaceSpec := models.NamespaceSpec{
			ID:          uuid.Must(uuid.NewRandom()),
			Name:        "game_jam",
			ProjectSpec: projectSpec,
		}
		jobSpec := models.JobSpec{
			Name: "transform-tables",
			Behavior: models.JobSpecBehavior{
				Notify: []models.JobSpecNotifier{
					{
						On: models.JobEventTypeFailure,
						Channels: []string{
							"slacker://@devs",
						},
					},
				},
			},
		}
		je := models.JobEvent{
			Type:  models.JobEventTypeFailure,
			Value: eventValues.GetFields(),
		}

		notifier := new(mock.Notifier)
		notifier.On("Notify", context.Background(), models.NotifyAttrs{
			Namespace: namespaceSpec,
			JobSpec:   jobSpec,
			JobEvent:  je,
			Route:     "@devs",
		}).Return(errors.New("failed to notify"))
		defer notifier.AssertExpectations(t)

		evtService := job.NewEventService(log, map[string]models.Notifier{
			"slacker": notifier,
		})
		err := evtService.Register(context.Background(), namespaceSpec, jobSpec, je)
		assert.Error(t, err, "failed to notify")
	})
}
