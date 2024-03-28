use aws_lambda_events::event::s3::{S3Entity, S3Event};
use aws_lambda_events::sqs::SqsEvent;
use aws_sdk_s3::Client as S3Client;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use routefinder::Router;
use tracing::log::*;

use std::collections::HashMap;

/// A simple structure to make deserializing test events for identification easier
///
/// See <fhttps://github.com/buoyant-data/oxbow/issues/8>
#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TestEvent {
    event: String,
}

/// Convert the given [aws_lambda_events::sqs::SqsEvent] to a collection of
///  [aws_lambda_events::s3::S3EventRecord] entities. This is mostly useful for handling S3 Bucket
///  Notifications which have been passed into SQS
///
///  In the case where the [aws_lambda_events::sqs::SqsEvent] contains an `s3:TestEvent` which is
///  fired when S3 Bucket Notifications are first enabled, the event will be ignored to avoid
///  errorsin the processing pipeline
fn s3_from_sqs(event: SqsEvent) -> Result<S3Event, anyhow::Error> {
    let mut records = vec![];
    for record in event.records.iter() {
        /* each record is an SqsMessage */
        if let Some(body) = &record.body {
            match serde_json::from_str::<S3Event>(body) {
                Ok(s3event) => {
                    for s3record in s3event.records {
                        records.push(s3record.clone());
                    }
                }
                Err(err) => {
                    // if we cannot deserialize and the event is an s3::TestEvent, then we should
                    // just return empty records.
                    let test_event = serde_json::from_str::<TestEvent>(body);
                    // Early exit with the original error if we cannot parse the JSON at all
                    if test_event.is_err() {
                        return Err(err.into());
                    }

                    // Ignore the error on deserialization if the event ends up being an S3
                    // TestEvent which is fired when bucket notifications are originally configured
                    if "s3:TestEvent" != test_event.unwrap().event {
                        return Err(err.into());
                    }
                }
            };
        }
    }
    Ok(aws_lambda_events::s3::S3Event { records })
}

async fn function_handler(
    event: LambdaEvent<serde_json::Value>,
    client: &S3Client,
) -> Result<(), Error> {
    let input_pattern =
        std::env::var("INPUT_PATTERN").expect("You must define INPUT_PATTERN in the environment");
    let output_template = std::env::var("OUTPUT_TEMPLATE")
        .expect("You must define OUTPUT_TEMPLATE in the environment");

    let mut router = Router::new();
    let template = liquid::ParserBuilder::with_stdlib()
        .build()?
        .parse(&output_template)?;

    router.add(input_pattern, 1)?;

    let records = match serde_json::from_value::<SqsEvent>(event.payload.clone()) {
        Ok(sqs_event) => s3_from_sqs(sqs_event)?,
        Err(_) => serde_json::from_value(event.payload)?,
    };

    for entity in entities_from(records)? {
        debug!("Processing {entity:?}");

        if let Some(source_key) = entity.object.key {
            let parameters = add_builtin_parameters(captured_parameters(&router, &source_key)?);
            let output_key = template.render(&parameters)?;
            info!("Copying {source_key:?} to {output_key:?}");
            if let Some(bucket) = entity.bucket.name {
                debug!("Sending a copy request for {bucket} with {source_key} to {output_key}");
                let result = client
                    .copy_object()
                    .bucket(&bucket)
                    .copy_source(format!("{bucket}/{source_key}"))
                    .key(output_key)
                    .send()
                    .await?;
                debug!("Copied object: {result:?}");
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    let shared_config = aws_config::load_from_env().await;
    let client = S3Client::new(&shared_config);
    let client_ref = &client;

    let func = service_fn(move |event| async move { function_handler(event, client_ref).await });
    run(func).await
}

/// Return the deserialized and useful objects from the event payload
///
/// This function will apply a filter to make sure that it is only return objects which have been
/// put in this invocation
fn entities_from(event: S3Event) -> Result<Vec<S3Entity>, anyhow::Error> {
    Ok(event
        .records
        .into_iter()
        // only bother with the record if the key is present
        .filter(|r| r.s3.object.key.is_some())
        .map(|r| r.s3)
        .collect())
}

/// Take the source key and the already configured router in order to access a collection of
/// captured parameters in a HashMap format
fn captured_parameters<Handler>(
    router: &Router<Handler>,
    source_key: &str,
) -> Result<HashMap<String, String>, anyhow::Error> {
    let matches = router.matches(source_key);
    let mut data: HashMap<String, String> = HashMap::new();
    for capture in matches[0].captures().into_iter() {
        data.insert(capture.name().into(), capture.value().into());
    }
    Ok(data)
}

fn add_builtin_parameters(mut data: HashMap<String, String>) -> HashMap<String, String> {
    use chrono::Datelike;
    let now = chrono::Utc::now();
    data.insert("year".into(), format!("{}", now.year()));
    data.insert("month".into(), format!("{}", now.month()));
    data.insert("day".into(), format!("{}", now.day()));
    data.insert("ds".into(), format!("{}", now.format("%Y-%m-%d")));
    data.insert(
        "region".into(),
        std::env::var("AWS_REGION").unwrap_or("unknown".into()),
    );
    data
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_builtins() {
        let data = add_builtin_parameters(HashMap::new());
        assert!(data.contains_key("year"), "builtins needs `year`");
        assert!(data.contains_key("month"), "builtins needs `month`");
        assert!(data.contains_key("day"), "builtins needs `day`");
        assert!(data.contains_key("ds"), "builtins needs `ds`");
        assert!(data.contains_key("region"), "builtins needs `region`");
    }

    #[test]
    fn test_input_router() -> Result<(), anyhow::Error> {
        let input_pattern = "path/:ignore/:database/:table/1/:filename";
        let source_key = "path/testing-2023-08-18-07-05-df7d7bcc-3160-50da-8c4c-26952b11a4c/testdb/public.test_table/1/foobar.snappy.parquet";

        let mut router = Router::new();
        let _ = router.add(input_pattern, 1);

        assert_eq!(router.matches("test/key").len(), 0);
        let matches = router.matches(source_key);
        assert_eq!(matches.len(), 1);
        assert_eq!(
            matches[0].captures().get("filename"),
            Some("foobar.snappy.parquet")
        );
        Ok(())
    }

    #[test]
    fn test_valid_entities_from_event() -> Result<(), anyhow::Error> {
        let event = load_test_event()?;
        let objects = entities_from(event)?;
        assert_eq!(objects.len(), 1);
        assert!(objects[0].object.key.is_some());

        Ok(())
    }

    /**
     * Return a simple test event from the Lambda built-in test tool
     */
    fn load_test_event() -> Result<S3Event, anyhow::Error> {
        let raw_buf = r#"
{
  "Records": [
    {
      "eventVersion": "2.0",
      "eventSource": "aws:s3",
      "awsRegion": "us-east-1",
      "eventTime": "1970-01-01T00:00:00.000Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "EXAMPLE"
      },
      "requestParameters": {
        "sourceIPAddress": "127.0.0.1"
      },
      "responseElements": {
        "x-amz-request-id": "EXAMPLE123456789",
        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "testConfigRule",
        "bucket": {
          "name": "example-bucket",
          "ownerIdentity": {
            "principalId": "EXAMPLE"
          },
          "arn": "arn:aws:s3:::example-bucket"
        },
        "object": {
          "key": "test%2Fkey",
          "size": 1024,
          "eTag": "0123456789abcdef0123456789abcdef",
          "sequencer": "0A1B2C3D4E5F678901"
        }
      }
    }
  ]
}"#;

        let event: S3Event = serde_json::from_str(&raw_buf)?;
        Ok(event)
    }

    /**
     * Quickly validate that the liquid rendering of things works properly
     */
    #[test]
    fn test_rendering() {
        let template = liquid::ParserBuilder::with_stdlib()
            .build()
            .unwrap()
            .parse("databases/{{database}}/{{table | remove:'public.'}}/ds={{ds}}/{{filename}}")
            .unwrap();
        let mut parameters: HashMap<String, String> = HashMap::new();
        parameters = add_builtin_parameters(parameters);
        parameters.insert("database".into(), "oltp".into());
        parameters.insert("table".into(), "public.a_table".into());
        parameters.insert("filename".into(), "some.parquet".into());
        parameters.insert("ds".into(), "2023-09-05".into());
        let output_key = template.render(&parameters).unwrap();
        assert_eq!(
            output_key,
            "databases/oltp/a_table/ds=2023-09-05/some.parquet"
        );
    }
}
