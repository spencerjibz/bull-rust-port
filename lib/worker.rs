use crate::emitter::AsyncEventEmitter;
use crate::timer::Timer;
use crate::*;

struct Worker<'a> {
    pub id: String,
    pub queue: String,
    pub connection: RedisConnection<'a>,
    pub options: WorkerOptions,
    pub emitter: AsyncEventEmitter,
    pub timer: Timer,
    pub worker: WorkerOptions,
    pub closing: bool,
    pub closed: bool,
    pub running: bool,
    
}
