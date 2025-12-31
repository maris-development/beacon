use object_store::ObjectMeta;

use crate::event::{EventHandler, ObjectEvent, ObjectEventResult};

pub enum FileSystemEvent {
    Created(ObjectMeta),
    Modified(ObjectMeta),
    Deleted(object_store::path::Path),
}

pub struct FileSystemEventHandler;

impl EventHandler<FileSystemEvent> for FileSystemEventHandler {
    fn handle_event(input: FileSystemEvent) -> ObjectEventResult<Vec<ObjectEvent>> {
        match input {
            FileSystemEvent::Created(meta) => Ok(vec![ObjectEvent::Created(meta)]),
            FileSystemEvent::Modified(meta) => Ok(vec![ObjectEvent::Modified(meta)]),
            FileSystemEvent::Deleted(path) => Ok(vec![ObjectEvent::Deleted(path)]),
        }
    }
}
