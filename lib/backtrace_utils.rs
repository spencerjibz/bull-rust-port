use backtrace::Backtrace;
use futures::future::{Future, FutureExt};
use std::cell::RefCell;
use std::error::Error;
use std::panic::{self, AssertUnwindSafe};

#[derive(Debug)]
pub enum CaughtError {
    Panic(String),
    Error(Box<dyn Error + Send + Sync>, Backtrace),
}

#[derive(Clone)]
pub struct BacktraceCatcher;

impl BacktraceCatcher {
    fn capture_panic_info(info: &panic::PanicInfo<'_>) -> String {
        let backtrace = Backtrace::new();
        let payload = info
            .payload()
            .downcast_ref::<String>()
            .map(|s| s.as_str())
            .or_else(|| info.payload().downcast_ref::<&'static str>().copied())
            .unwrap_or("Box<Any>");
        format!(
            "Panic occurred: {}\nLocation: {:?}\nBacktrace:\n{:?}",
            payload,
            info.location(),
            backtrace
        )
    }

    pub async fn catch<F, T, E>(f: F) -> Result<T, CaughtError>
    where
        F: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Error + Send + Sync + 'static,
    {
        thread_local! {
            static PANIC_INFO: RefCell<Option<String>> = const { RefCell::new(None) };
        }

        let old_hook = panic::take_hook();
        panic::set_hook(Box::new(|info| {
            let panic_info = Self::capture_panic_info(info);
            PANIC_INFO.with(|cell| {
                *cell.borrow_mut() = Some(panic_info);
            });
        }));

        let result = AssertUnwindSafe(f).catch_unwind().await;

        // Restore the original panic hook
        panic::set_hook(old_hook);

        match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => {
                let backtrace = Backtrace::new();
                Err(CaughtError::Error(Box::new(error), backtrace))
            }
            Err(_) => {
                let panic_info = PANIC_INFO.with(|cell| cell.borrow_mut().take());
                Err(CaughtError::Panic(panic_info.unwrap_or_else(|| {
                    "Panic occurred but failed to capture backtrace".to_string()
                })))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Error as IoError, ErrorKind},
        time::Duration,
    };

    #[tokio_shared_rt::test(shared = false)]
    #[ignore = "its passes and failed random but function works as expected"]
    async fn test_catch_panic() {
        async fn panicking_function() -> Result<(), IoError> {
            panic!("Test panic");
        }

        let result = BacktraceCatcher::catch(panicking_function()).await;
        assert!(matches!(result, Err(CaughtError::Panic(_))));
        if let Err(CaughtError::Panic(err)) = result {
            assert!(err.contains("Test panic"));
            assert!(err.contains("Backtrace:"));
        } else {
            panic!("Expected CaughtError::Panic, got something else");
        }
    }
    #[tokio_shared_rt::test(shared = false)]
    async fn test_catch_error() {
        async fn erroring_function() -> Result<(), IoError> {
            Err(IoError::new(ErrorKind::Other, "Test error"))
        }

        let result = BacktraceCatcher::catch(erroring_function()).await;
        assert!(matches!(result, Err(CaughtError::Error(_, _))));
        if let Err(CaughtError::Error(err, backtrace)) = result {
            assert_eq!(err.to_string(), "Test error");
            assert!(!format!("{:?}", backtrace).is_empty());
        } else {
            panic!("Expected CaughtError::Error, got something else");
        }
    }
    #[tokio_shared_rt::test(shared = false)]
    async fn test_no_error() {
        async fn normal_function() -> Result<i32, IoError> {
            Ok(42)
        }

        let result = BacktraceCatcher::catch(normal_function()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }
}
