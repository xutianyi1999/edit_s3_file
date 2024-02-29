#![feature(once_cell_try)]
#![feature(lazy_cell)]

use std::sync::{LazyLock, OnceLock};

use anyhow::{anyhow, ensure, Result};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_types::region::Region;
use aws_types::SdkConfig;
use serde::Deserialize;
use tokio::runtime::Runtime;

pub struct Part {
    index: i64,
    data: Option<Vec<u8>>,
}

impl Part {
    pub fn new(index: i64, data: Vec<u8>) -> Self {
        Part {
            index,
            data: Some(data),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

pub const PATH_ENV: &'static str = "S3_STORE_CONFIG";
// 1GB
const PART_SIZE: i64 = 1024 * 1024 * 1024;
static CLIENT: OnceLock<(String, Client)> = OnceLock::new();

static RT: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
});

pub fn modify(
    key: &str,
    mut modify_part: Part,
) -> Result<()> {
    let (bucket, client) = CLIENT.get_or_try_init(|| {
        let path = std::env::var(PATH_ENV)?;
        let config: S3Config = serde_json::from_reader(std::fs::File::open(path)?)?;

        let mut builder = SdkConfig::builder()
            .endpoint_url(config.endpoint)
            .region(Region::new(config.region));

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
        let obj = client.get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;

        let obj_len = obj.content_length().ok_or_else(|| anyhow!("{} content length is empty", key))?;
        let modify_part_len = modify_part.data.as_ref().unwrap().len() as i64;
        ensure!(modify_part.index + modify_part_len <= obj_len);

        let upload_out = client.create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;

        let upload_id = upload_out
            .upload_id()
            .ok_or_else(|| anyhow!("{}, must need upload id", key))?;

        let mut part_num = 1;
        let mut offset = 0;
        let mut etags = Vec::new();

        while offset < obj_len {
            let mut end;

            if offset == modify_part.index {
                end = modify_part.index + modify_part_len;

                println!("upload, part_num: {}", part_num);

                let etag = client.upload_part()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(upload_id)
                    .part_number(part_num)
                    .body(ByteStream::from(modify_part.data.take().unwrap()))
                    .send()
                    .await?
                    .e_tag
                    .ok_or_else(|| anyhow!("{} must need e_tag", key))?;

                etags.push(etag);
            } else {
                end = std::cmp::min(offset + PART_SIZE, obj_len);

                if modify_part.index > offset &&
                    modify_part.index < end {
                    end = modify_part.index;
                }

                println!("copy, part_num: {}, range: {}-{}", part_num, offset, end -1);

                let etag = client.upload_part_copy()
                    .copy_source(format!("/{}/{}", bucket, key))
                    .copy_source_range(format!("bytes={}-{}", offset, end - 1))
                    .bucket(bucket)
                    .key(key)
                    .upload_id(upload_id)
                    .part_number(part_num)
                    .send()
                    .await?
                    .copy_part_result
                    .ok_or_else(|| anyhow!("{} must need copy part result", key))?
                    .e_tag
                    .ok_or_else(|| anyhow!("{} must need e_tag", key))?;

                etags.push(etag);
            }

            offset = end;
            part_num += 1;
        }

        let parts = etags.into_iter()
            .enumerate()
            .map(|(i, e_tag)| {
                CompletedPart::builder()
                    .part_number(i as i32 + 1)
                    .e_tag(e_tag)
                    .build()
            })
            .collect::<Vec<_>>();

        client.complete_multipart_upload()
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await?;

        Ok(())
    })
}