#![feature(once_cell_try)]
#![feature(lazy_cell)]

use std::sync::{LazyLock, OnceLock};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use aws_types::region::Region;
use aws_types::SdkConfig;
use serde::Deserialize;
use anyhow::Result;
use tokio::runtime::Runtime;

pub struct Part<'a> {
    index: u64,
    data: &'a [u8],
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub region: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

pub const PATH_ENV: &'static str = "S3_STORE_CONFIG";
static CLIENT: OnceLock<(String, Client)> = OnceLock::new();

static RT: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
});

pub fn modify(
    key: &str,
    parts: &[Part],
) -> Result<()> {
    let (bucket, client) = CLIENT.get_or_try_init(|| {
        let path = std::env::var(PATH_ENV)?;
        let config: S3Config = serde_json::from_reader(std::fs::File::open(path)?)?;

        let mut builder = SdkConfig::builder()
            .endpoint_url(config.endpoint)
            .region(config.region.map(Region::new));

        if let (Some(ak), Some(sk)) = (config.access_key, config.secret_key) {
            builder = builder.credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                ak,
                sk,
                None,
                None,
                "Static",
            )))
        }
        Result::<_, anyhow::Error>::Ok((config.bucket, Client::new(&builder.build())))
    })?;

    RT.block_on(async {
        let parts = client.list_parts()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;

        let parts = parts.parts.unwrap();

        for part in parts {
            println!("num: {:?}, size: {:?}", part.part_number, part.size)
        }
        Ok(())
    })
}