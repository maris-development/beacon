use tokio::sync::{Mutex, Notify};

#[derive(Debug)]
pub struct AsyncOnce<T> {
    value: Mutex<Option<T>>,
    notify: Notify,
}

impl<T> AsyncOnce<T> {
    pub fn new() -> Self {
        Self {
            value: Mutex::new(None),
            notify: Notify::new(),
        }
    }

    pub async fn get(&self) -> T
    where
        T: Clone,
    {
        loop {
            if let Some(v) = self.value.lock().await.clone() {
                return v;
            }
            self.notify.notified().await;
        }
    }

    pub async fn initialize(&self, value: T) -> Result<(), Box<dyn std::error::Error>> {
        let mut guard = self.value.lock().await;
        if guard.is_some() {
            return Err("Already initialized".into());
        }
        *guard = Some(value);
        drop(guard);
        self.notify.notify_waiters();
        Ok(())
    }
}

impl<T> Default for AsyncOnce<T> {
    fn default() -> Self {
        Self::new()
    }
}
