use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
type BackoffFn = dyn Fn(i64) -> BoxedFn + Send + Sync;
pub type BoxedFn = Box<dyn Fn(i64) -> i64 + Send + Sync>;
use anyhow::Ok;

use crate::*;

//import lazy_static
#[derive(Clone, Default)]
pub struct BackOff {
    pub builtin_strategies: HashMap<&'static str, Arc<BackoffFn>>,
}

impl BackOff {
    pub fn new() -> Self {
        let mut backoff = BackOff::default();
        backoff.register("exponential", |delay: i64| {
            Box::new(move |atempts: i64| -> i64 { 2_i64.pow(atempts as u32) * delay })
        });

        backoff.register("fixed", |delay: i64| Box::new(move |_attempts| delay));
        backoff
    }

    pub fn register(
        &mut self,
        name: &'static str,
        strategy: impl Fn(i64) -> Box<dyn Fn(i64) -> i64 + Send + Sync> + 'static + Send + Sync,
    ) {
        self.builtin_strategies.insert(name, Arc::new(strategy));
    }
    pub fn normalize(backoff: (i64, Option<BackOffOptions>)) -> Option<BackOffOptions> {
        if backoff.0 == 0 {
            return None;
        }
        match backoff {
            (num, None) => {
                let opts = BackOffOptions {
                    delay: Some(num),
                    type_: Some("exponential".to_string()),
                };
                Some(opts)
            }
            (num, Some(opts)) => Some(opts),
            _ => None,
        }
    }
    pub fn calculate(
        &self,
        backoff_opts: Option<BackOffOptions>,
        atempts: i64,
        custom_strategy: Option<BoxedFn>,
    ) -> anyhow::Result<Option<i64>> {
        if let Some(opts) = backoff_opts {
            let strategy = self.lookup_strategy(opts, custom_strategy)?;
            let result = strategy(atempts);
            return Ok(Some(result));
        }

        Ok(None)
    }

    pub fn has_strategy(&self, key: &str) -> bool {
        self.builtin_strategies.contains_key(key)
    }

    pub fn lookup_strategy(
        &self,
        backoff: BackOffOptions,
        custom_strategy: Option<BoxedFn>,
    ) -> anyhow::Result<BoxedFn>
where {
        if let (Some(t)) = backoff.type_ {
            if let Some(strategy) = self.builtin_strategies.get(t.as_str()) {
                return Ok(strategy(backoff.delay.unwrap()));
            }

            return Err(anyhow::anyhow!("Unknown backoff strategy: {}", t));
        }

        if let Some(strategy) = custom_strategy {
            return Ok(strategy);
        }

        Err(anyhow::anyhow!("No backoff strategy specified"))
    }
}

//implement std::fmt::Debug for Ba
impl std::fmt::Debug for BackOff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackOff")
            .field("builtin_strategies", &self.builtin_strategies.keys())
            .finish()
    }
}
// write tests for this module

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_expotential_backoff() {
        let mut backoff = BackOff::new();
        let strategy = backoff.lookup_strategy(
            BackOffOptions {
                delay: Some(100),
                type_: Some("exponential".to_string()),
            },
            None,
        );
        //println!("{:?}", backoff);
        assert_eq!(strategy.unwrap()(1), 200);
    }

    #[test]
    fn test_fixed_back() {
        let mut backoff = BackOff::new();
        let strategy = backoff.lookup_strategy(
            BackOffOptions {
                delay: Some(100),
                type_: Some("fixed".to_string()),
            },
            None,
        );

        assert_eq!(strategy.unwrap()(200), 100);
    }
}
