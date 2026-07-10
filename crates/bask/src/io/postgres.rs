/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! Postgres record sink: each batch is serialized to CSV and streamed into the target
//! table with `COPY ... FROM STDIN`, the fast bulk-load path. Targets are written as
//! `postgres://<connection>#<table>`; connections use `NoTls`.

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use futures::SinkExt;
use tokio_postgres::{Client, NoTls};

use super::{Keyed, Sink, SinkRegistry, Target};

pub struct PostgresSink {
    conn_str: String,
    table: String,
    client: Option<Client>,
    driver: Option<tokio::task::JoinHandle<()>>,
}

impl PostgresSink {
    pub fn new(conn_str: impl Into<String>, table: impl Into<String>) -> Self {
        PostgresSink {
            conn_str: conn_str.into(),
            table: table.into(),
            client: None,
            driver: None,
        }
    }

    async fn client(&mut self) -> anyhow::Result<&Client> {
        if self.client.is_none() {
            let (client, connection) = tokio_postgres::connect(&self.conn_str, NoTls).await?;
            self.driver = Some(tokio::spawn(async move {
                let _ = connection.await;
            }));
            self.client = Some(client);
        }
        Ok(self.client.as_ref().expect("connected above"))
    }
}

#[async_trait]
impl Sink<RecordBatch> for PostgresSink {
    async fn write(&mut self, item: &Keyed<RecordBatch>) -> anyhow::Result<()> {
        let batch = item.value.clone();
        let csv = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<u8>> {
            let mut buf = Vec::new();
            let mut writer = arrow::csv::WriterBuilder::new()
                .with_header(false)
                .build(&mut buf);
            writer.write(&batch)?;
            drop(writer);
            Ok(buf)
        })
        .await??;

        let statement = format!("COPY {} FROM STDIN WITH (FORMAT csv)", self.table);
        let client = self.client().await?;
        let sink = client.copy_in(statement.as_str()).await?;
        futures::pin_mut!(sink);
        sink.send(Bytes::from(csv)).await?;
        sink.finish().await?;
        Ok(())
    }

    async fn finish(&mut self) -> anyhow::Result<()> {
        if let Some(driver) = self.driver.take() {
            driver.abort();
        }
        self.client = None;
        Ok(())
    }
}

pub fn register_sink_builtins(registry: &mut SinkRegistry<RecordBatch>) {
    registry.register_scheme(&["postgres", "postgresql"], |t: &Target, _o| {
        let (conn, table) = t.rest.rsplit_once('#').ok_or_else(|| {
            anyhow::anyhow!("postgres target needs a table: postgres://<connection>#<table>")
        })?;
        let conn_str = format!("{}://{}", t.scheme, conn);
        Ok(Box::new(PostgresSink::new(conn_str, table)) as Box<dyn Sink<RecordBatch>>)
    });
}
