use object_store::ObjectMeta;

#[derive(Debug, Clone)]
pub enum ObjectEvent {
    Created(ObjectMeta),
    Modified(ObjectMeta),
    Deleted(object_store::path::Path), // path
}
