use serde::Serialize;

/// One line of machine-readable progress.
///
/// Each event is printed as one JSON object per line when `--json-events` is on.
#[derive(Debug, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    Started {
        job_id: String,
        provider: String,
        total_files: u64,
        total_bytes: u64,
    },
    Progress {
        job_id: String,
        path: String,
        bytes_done: u64,
        bytes_total: u64,
    },
    FileFinished {
        job_id: String,
        path: String,
        bytes: u64,
    },
    /// Conteudo conferido contra o checksum que a origem declarou.
    ///
    /// A ausencia deste evento para um arquivo significa que a origem nao
    /// declarou checksum nenhum, nao que o arquivo esteja errado.
    Verified {
        job_id: String,
        path: String,
        algorithm: String,
    },
    Retry {
        job_id: String,
        path: String,
        chunk: u64,
        attempt: u32,
        reason: String,
    },
    Error {
        job_id: String,
        message: String,
    },
    Finished {
        job_id: String,
        total_files: u64,
        total_bytes: u64,
    },
}

#[derive(Clone, Debug)]
pub struct EventSink {
    json_events: bool,
}

impl EventSink {
    pub fn new(json_events: bool) -> Self {
        Self { json_events }
    }

    pub fn emit(&self, event: Event) {
        if self.json_events {
            match serde_json::to_string(&event) {
                Ok(line) => println!("{line}"),
                Err(e) => {
                    println!(r#"{{"event":"error","message":"failed to encode event: {e}"}}"#)
                }
            }
        } else {
            tracing::info!("{event:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_verified_as_ndjson_event() {
        let event = Event::Verified {
            job_id: "job-1".to_string(),
            path: "file.bin".to_string(),
            algorithm: "sha256".to_string(),
        };
        let value = serde_json::to_value(event).unwrap();
        assert_eq!(value["event"], "verified");
        assert_eq!(value["algorithm"], "sha256");
    }

    #[test]
    fn serializes_retry_as_ndjson_event() {
        let event = Event::Retry {
            job_id: "job-1".to_string(),
            path: "file.bin".to_string(),
            chunk: 0,
            attempt: 2,
            reason: "Range ignored".to_string(),
        };
        let value = serde_json::to_value(event).unwrap();
        assert_eq!(value["event"], "retry");
        assert_eq!(value["attempt"], 2);
    }
}
