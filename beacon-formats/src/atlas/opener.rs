use datafusion::datasource::{
    listing::PartitionedFile,
    physical_plan::{FileMeta, FileOpenFuture, FileOpener},
};

pub struct AtlasOpener {}

impl FileOpener for AtlasOpener {
    fn open(
        &self,
        file_meta: FileMeta,
        file: PartitionedFile,
    ) -> datafusion::error::Result<FileOpenFuture> {
        todo!()
    }
}
