use anyhow::Result;
use tokio;

#[tokio::main]
async fn main() -> Result<()> {
    inner::main().await
}

#[cfg(all(feature = "cfg", feature = "kafka"))]
mod inner {
    use super::*;
    pub async fn main() -> Result<()> {
        println!("use kafka utils");
        Ok(())
    }
}

#[cfg(not(all(feature = "cfg", feature = "kafka")))]
mod inner {
    use super::*;

    pub async fn main() -> Result<()> {
        println!("Please pass --features cfg,kafka-utils to cargo when trying this example.");

        Ok(())
    }
}