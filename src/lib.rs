//! Swiss army knife utilities for all kind of Avantis rust projects

#[cfg(feature = "config")]
pub mod config;
#[cfg(feature = "db_sqlx")]
pub mod db;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "pagination")]
pub mod pagination;
#[cfg(feature = "redis")]
pub mod redis;
