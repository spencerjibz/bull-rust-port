 use std::time::Duration;
 use tokio::time::{sleep, Instant};


struct Timer {
    interval: Duration,
    _ok:bool,
    _task: tokio::task::JoinHandle<()>,
}


impl Timer {
    pub fn new(interval: Duration, mut callback: Box<dyn FnMut() + Send + Sync>) -> Self {
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            
                
                callback();
            
        });
        Self {
            interval,
        
            _ok: true,
            _task: task,
        }
    }
    



    pub fn stop(&mut self) {
        self._ok = false;
        self._task.abort();
    }
}