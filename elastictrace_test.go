package elastic

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch"
	"github.com/elastic/go-elasticsearch/esapi"
	"github.com/stretchr/testify/assert"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/mocktracer"
)

const debug = false

func TestMain(m *testing.M) {
	_, ok := os.LookupEnv("INTEGRATION")
	if !ok {
		fmt.Println("--- SKIP: to enable integration test, set the INTEGRATION environment variable")
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestClientV8(t *testing.T) {
	assert := assert.New(t)
	mt := mocktracer.Start()
	defer mt.Stop()

	var tp http.RoundTripper
	tp = NewHTTPClient(WithServiceName("my-es-service"))
	cfg := elasticsearch.Config{
		Transport: tp,
		Addresses: []string{
			"http://127.0.0.1:9200",
		},
	}
	client, err := elasticsearch.NewClient(cfg)
	assert.NoError(err)

	_, err = esapi.IndexRequest{
		Index:      "twitter",
		DocumentID: "1",
		Body:       strings.NewReader(`{"user": "test", "message": "hello"}`),
	}.Do(context.TODO(), client)

	assert.NoError(err)

	mt.Reset()
	_, err = esapi.GetRequest{
		Index:      "twitter",
		DocumentID: "1",
	}.Do(context.TODO(), client)
	assert.NoError(err)
	checkGETTrace(assert, mt)

	mt.Reset()
	_, err = esapi.GetRequest{
		Index:      "not-real-index",
		DocumentID: "1",
	}.Do(context.TODO(), client)
	assert.Error(err)
	checkErrTrace(assert, mt)

}

func checkPUTTrace(assert *assert.Assertions, mt mocktracer.Tracer) {
	span := mt.FinishedSpans()[0]
	assert.Equal("my-es-service", span.Tag(ext.ServiceName))
	assert.Equal("PUT /twitter/tweet/?", span.Tag(ext.ResourceName))
	assert.Equal("/twitter/tweet/1", span.Tag("elasticsearch.url"))
	assert.Equal("PUT", span.Tag("elasticsearch.method"))
	assert.Equal(`{"user": "test", "message": "hello"}`, span.Tag("elasticsearch.body"))
}

func checkGETTrace(assert *assert.Assertions, mt mocktracer.Tracer) {
	span := mt.FinishedSpans()[0]
	assert.Equal("my-es-service", span.Tag(ext.ServiceName))
	assert.Equal("GET /twitter/tweet/?", span.Tag(ext.ResourceName))
	assert.Equal("/twitter/tweet/1", span.Tag("elasticsearch.url"))
	assert.Equal("GET", span.Tag("elasticsearch.method"))
}

func checkErrTrace(assert *assert.Assertions, mt mocktracer.Tracer) {
	span := mt.FinishedSpans()[0]
	assert.Equal("my-es-service", span.Tag(ext.ServiceName))
	assert.Equal("GET /not-real-index/_all/?", span.Tag(ext.ResourceName))
	assert.Equal("/not-real-index/_all/1", span.Tag("elasticsearch.url"))
	assert.NotEmpty(span.Tag(ext.Error))
	assert.Equal("*errors.errorString", fmt.Sprintf("%T", span.Tag(ext.Error).(error)))
}
