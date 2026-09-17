use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, memory::InMemory, path::Path,
};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Default)]
pub struct FaultStore {
    inner: InMemory,
    target: parking_lot::Mutex<String>,
    armed: AtomicBool,
    pub entered: tokio::sync::Notify,
}
impl std::fmt::Display for FaultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("attention-fault")
    }
}
impl FaultStore {
    pub fn arm(&self, prefix: &str) {
        *self.target.lock() = prefix.into();
        self.armed.store(true, Ordering::SeqCst);
    }
}
#[async_trait::async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        let matched = path.as_ref().contains(self.target.lock().as_str())
            && self.armed.swap(false, Ordering::SeqCst);
        let result = self.inner.put_opts(path, payload, options).await?;
        if matched {
            self.entered.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(result)
    }
    async fn put_multipart_opts(
        &self,
        p: &Path,
        o: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(p, o).await
    }
    async fn get_opts(&self, p: &Path, o: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(p, o).await
    }
    fn delete_stream(
        &self,
        p: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(p)
    }
    fn list(&self, p: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(p)
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, a: &Path, b: &Path, o: CopyOptions) -> Result<()> {
        self.inner.copy_opts(a, b, o).await
    }
}
