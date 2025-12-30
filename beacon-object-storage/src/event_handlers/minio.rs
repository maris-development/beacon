use crate::event::{EventHandler, ObjectEvent, ObjectEventResult};

pub type MinIOEvent = serde_json::Value; // Webhook payload from MinIO

pub struct MinIOEventHandler;

impl EventHandler<MinIOEvent> for MinIOEventHandler {
    fn handle_event(input: MinIOEvent) -> ObjectEventResult<Vec<ObjectEvent>> {
        // ToDo: Parse the MinIO event JSON payload to extract object events and then map them to our ObjectEvent enum.
        todo!()
    }
}
