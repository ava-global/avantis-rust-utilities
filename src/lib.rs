//! Swiss army knife utilities for all kind of Avantis rust projects

#[cfg(feature = "cfg")]
pub mod config;
#[cfg(feature = "db")]
pub mod db;
#[cfg(feature = "redis-utils")]
pub mod redis;
