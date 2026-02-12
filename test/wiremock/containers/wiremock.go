package containers

import (
	"context"
	"net/http"
	"strings"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type WiremockContainer struct {
	container testcontainers.Container
	baseURL   string
}

type WiremockMapping struct {
	Name    string
	Content string
}

func StartWiremockContainer(ctx context.Context, mappings []WiremockMapping) (*WiremockContainer, error) {
	files := make([]testcontainers.ContainerFile, len(mappings))
	for i, mapping := range mappings {
		files[i] = testcontainers.ContainerFile{
			ContainerFilePath: "/home/wiremock/mappings/" + mapping.Name,
			HostFilePath:      "/home/wiremock/mappings/" + mapping.Name,
			FileMode:          0o644,
			Reader:            strings.NewReader(mapping.Content),
		}
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "wiremock/wiremock:3.13.2-alpine",
			Env:          map[string]string{"WIREMOCK_OPTIONS": "--disable-banner"},
			ExposedPorts: []string{"8080/tcp"},
			Files:        files,
			WaitingFor: wait.ForHTTP("/__admin/health").WithPort("8080").WithMethod("GET").WithStatusCodeMatcher(func(status int) bool {
				return status == http.StatusOK
			}),
		},
		Started: true,
	})
	if err != nil {
		return nil, err
	}

	baseURL, err := container.PortEndpoint(ctx, "8080/tcp", "http")
	if err != nil {
		return nil, err
	}

	return &WiremockContainer{
		container: container,
		baseURL:   baseURL,
	}, nil
}

func (w *WiremockContainer) BaseURL() string {
	return w.baseURL
}

func (w *WiremockContainer) Stop(ctx context.Context) error {
	return w.container.Terminate(ctx)
}
