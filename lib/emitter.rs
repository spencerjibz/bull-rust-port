use crate::*;
use bincode;
use std::{collections::HashMap, sync::Arc};
use tokio::task::{self, JoinHandle};

use futures::future::{BoxFuture, Future, FutureExt};
use uuid::Uuid;

pub type AsyncCB = dyn Fn(Vec<u8>) -> BoxFuture<'static, ()> + Send + Sync + 'static;
pub struct AsyncListener {
    callback: Arc<AsyncCB>,
    limit: Option<u64>,
    id: String,
}

#[derive(Default)]
pub struct AsyncEventEmitter {
    pub listeners: HashMap<String, Vec<AsyncListener>>,
}

impl AsyncEventEmitter {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn emit<T>(&mut self, event: &str, value: T) -> anyhow::Result<()>
    where
        T: Serialize + Deserialize<'static> + Send + Sync + 'static + std::fmt::Debug,
    {
        let mut callback_handlers: Vec<_> = Vec::new();

        if let Some(listeners) = self.listeners.get_mut(event) {
            let mut listeners_to_remove: Vec<usize> = Vec::new();
            for (index, listener) in listeners.iter_mut().enumerate() {
                let bytes: Vec<u8> = bincode::serialize(&value).unwrap();

                let callback = Arc::clone(&listener.callback);

                match listener.limit {
                    None => {
                        callback_handlers.push(task::spawn(async move {
                            callback(bytes).await;
                        }));
                    }
                    Some(limit) => {
                        if limit != 0 {
                            callback_handlers
                                .push(task::spawn(async move { callback(bytes).await }));
                            listener.limit = Some(limit - 1);
                        } else {
                            listeners_to_remove.push(index);
                        }
                    }
                }
            }

            // Reverse here so we don't mess up the ordering of the vector
            for index in listeners_to_remove.into_iter().rev() {
                listeners.remove(index);
            }
        }

        for handles in callback_handlers {
            handles.await?;
        }

        Ok(())
    }

    pub fn remove_listener(&mut self, id_to_delete: &str) -> Option<String> {
        for (_, event_listeners) in self.listeners.iter_mut() {
            if let Some(index) = event_listeners
                .iter()
                .position(|listener| listener.id == id_to_delete)
            {
                event_listeners.remove(index);
                return Some(id_to_delete.to_string());
            }
        }

        None
    }

    pub async fn on_limited<F, T>(&mut self, event: &str, limit: Option<u64>, callback: F) -> String
    where
        for<'de> T: Deserialize<'de> + std::fmt::Debug,
        F: Fn(T) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        let id = Uuid::new_v4().to_string();
        let parsed_callback = move |bytes: Vec<u8>| {
            let value: T = bincode::deserialize(&bytes).unwrap();

            callback(value)
        };

        let listener = AsyncListener {
            id: id.clone(),
            limit,
            callback: Arc::new(parsed_callback),
        };

        match self.listeners.get_mut(event) {
            Some(callbacks) => {
                callbacks.push(listener);
            }
            None => {
                self.listeners.insert(event.to_string(), vec![listener]);
            }
        }

        id
    }
    pub async fn once<F, T>(&mut self, event: &str, callback: F) -> String
    where
        for<'de> T: Deserialize<'de> + std::fmt::Debug,
        F: Fn(T) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        self.on_limited(event, Some(1), callback).await
    }
    pub async fn on<F, T>(&mut self, event: &str, callback: F) -> String
    where
        for<'de> T: Deserialize<'de> + std::fmt::Debug,
        F: Fn(T) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        self.on_limited(event, None, callback).await
    }
}

// test the AsyncEventEmitter
// implment fmt::Debug for AsyncEventListener
use std::fmt;
impl fmt::Debug for AsyncListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncListener")
            .field("id", &self.id)
            .field("limit", &self.limit)
            .finish()
    }
}

// implement fmt::Debug   for AsyncEventEmitter
impl fmt::Debug for AsyncEventEmitter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncEventEmitter")
            .field("listeners", &self.listeners)
            .finish()
    }
}
#[cfg(test)]

mod tests {
    use std::cell::RefCell;

    use super::*;
    use bincode::{options, DefaultOptions};
    use tokio::test;

    #[derive(Serialize, Deserialize, Debug)]
    struct Date {
        month: String,
        day: String,
    }
    #[derive(Serialize, Deserialize, Debug)]
    struct Time {
        hour: String,
        minute: String,
    }
    #[derive(Serialize, Deserialize, Debug)]
    struct DateTime(Date, Time);

    #[tokio::test]

    async fn test_async_event() {
        let mut event_emitter = AsyncEventEmitter::new();

        let date = Date {
            month: "January".to_string(),
            day: "Tuesday".to_string(),
        };

        event_emitter
            .on("LOG_DATE", |date: Date| {
                async move { /*Do something here */ }.boxed()
            })
            .await;
        event_emitter
            .on("LOG_DATE", |date: Date| {
                async move { println!(" emitted data: {:#?}", date) }.boxed()
            })
            .await;
        event_emitter.emit("LOG_DATE", date).await;
        println!("{:#?}", event_emitter);
        assert!(event_emitter.listeners.get("LOG_DATE").is_some());
    }

    #[tokio::test]
    async fn test_emit_multiple_args() {
        let mut event_emitter = AsyncEventEmitter::new();
        let name = "LOG_DATE".to_string();
        event_emitter
            .on("LOG_DATE", |tup: (Date, String)| {
                async move { println!("{:#?}", tup) }.boxed()
            })
            .await;

        event_emitter
            .emit(
                "LOG_DATE",
                (
                    Date {
                        month: "January".to_string(),
                        day: "Tuesday".to_string(),
                    },
                    name,
                ),
            )
            .await;

        println!("{:?}", event_emitter.listeners);
    }

    #[tokio::test]
    async fn bincode_encode_test() {
        let example = DateTime(
            Date {
                month: "January".to_string(),
                day: "Tuesday".to_string(),
            },
            Time {
                hour: "12".to_string(),
                minute: "30".to_string(),
            },
        );
        let encoded: Vec<u8> = bincode::serialize(&example).unwrap();
        let decoded: (Date, Time) = bincode::deserialize(&encoded).unwrap();
        #[derive(Serialize, Deserialize, Debug)]
        struct DateTimeContainer {
            data: RefCell<DateTime>,
        }

        let mut container = DateTimeContainer {
            data: RefCell::new(example),
        };

        println!("{:#?}", container.data);
    }
}
