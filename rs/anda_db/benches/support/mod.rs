use async_trait::async_trait;
use futures::{StreamExt, stream::BoxStream};
use object_store::{path::Path, *};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Default)]
pub struct InstrumentedStore {
    inner: memory::InMemory,
    pub latency_ms: AtomicU64,
    pub calls: [AtomicU64; 4],
}
impl InstrumentedStore {
    async fn pause(&self, op: usize) {
        self.calls[op].fetch_add(1, Ordering::Relaxed);
        let ms = self.latency_ms.load(Ordering::Relaxed);
        if ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        }
    }
    pub fn reset(&self, latency_ms: u64) {
        for counter in &self.calls {
            counter.store(0, Ordering::Relaxed);
        }
        self.latency_ms.store(latency_ms, Ordering::Relaxed);
    }
    pub fn counts(&self) -> [u64; 4] {
        std::array::from_fn(|i| self.calls[i].load(Ordering::Relaxed))
    }
}
impl std::fmt::Display for InstrumentedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InstrumentedStore")
    }
}
#[derive(Debug, Clone)]
pub struct Store(pub Arc<InstrumentedStore>);
impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
#[async_trait]
impl ObjectStore for Store {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.0.pause(1).await;
        self.0.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.0.pause(1).await;
        self.0.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &Path, opts: GetOptions) -> Result<GetResult> {
        self.0.pause(0).await;
        self.0.inner.get_opts(location, opts).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let this = self.clone();
        locations
            .then(move |path| {
                let this = this.clone();
                async move {
                    let path = path?;
                    this.0.pause(2).await;
                    this.0.inner.delete(&path).await?;
                    Ok(path)
                }
            })
            .boxed()
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.0.calls[3].fetch_add(1, Ordering::Relaxed);
        self.0.inner.list(prefix)
    }
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.0.calls[3].fetch_add(1, Ordering::Relaxed);
        self.0.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.0.pause(3).await;
        self.0.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> Result<()> {
        self.0.pause(1).await;
        self.0.inner.copy_opts(from, to, opts).await
    }
}
