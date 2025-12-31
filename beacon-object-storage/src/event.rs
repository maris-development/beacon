use object_store::ObjectMeta;

pub type EventError = Box<dyn std::error::Error + Send + Sync>;
pub type ObjectEventResult<T> = Result<T, EventError>;

pub enum ObjectEvent {
    Created(ObjectMeta),
    Modified(ObjectMeta),
    Deleted(object_store::path::Path), // path
}

pub trait EventHandler<I> {
    fn handle_event(input: I) -> ObjectEventResult<Vec<ObjectEvent>>;
}
