// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#[macro_use]
extern crate criterion;
extern crate arrow;
extern crate datafusion;

mod data_utils;
use crate::criterion::Criterion;
use data_utils::{create_table_provider, create_table_provider_2};
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use parking_lot::Mutex;
use std::sync::Arc;
use criterion::async_executor::FuturesExecutor;
use tokio::runtime::Runtime;

fn query(ctx: Arc<Mutex<SessionContext>>, rt: &Runtime, sql: &str) {
    let df = rt.block_on(ctx.lock().sql(sql)).unwrap();
    criterion::black_box(rt.block_on(df.collect()).unwrap());
}

async fn query3(ctx: Arc<Mutex<SessionContext>>, sql: &str) {
    let df = ctx.lock().sql(sql).await.unwrap();
    df.collect().await.unwrap();
}

async fn query_2(ctx: &SessionContext, sql: &str) {
    let df = ctx.sql(sql).await.unwrap();
    df.collect().await.unwrap();
}


fn create_context(
    partitions_len: usize,
    array_len: usize,
    batch_size: usize,
) -> Result<Arc<Mutex<SessionContext>>> {
    let ctx = SessionContext::new();
    let provider = create_table_provider(partitions_len, array_len, batch_size)?;
    ctx.register_table("t", provider)?;
    Ok(Arc::new(Mutex::new(ctx)))
}

fn create_context_3(
    partitions_len: usize,
    array_len: usize,
    batch_size: usize,
) -> Result<Arc<Mutex<SessionContext>>> {
    let ctx = SessionContext::new();
    let provider = create_table_provider_2(partitions_len, array_len, batch_size)?;
    ctx.register_table("t", provider)?;
    Ok(Arc::new(Mutex::new(ctx)))
}

fn create_context_2(
    partitions_len: usize,
    array_len: usize,
    batch_size: usize,
) -> Result<SessionContext> {
    let ctx = SessionContext::new();
    let provider = create_table_provider_2(partitions_len, array_len, batch_size)?;
    ctx.register_table("t", provider)?;
    Ok(ctx)
}

fn criterion_benchmark(c: &mut Criterion) {
    let partitions_len = 8;
    let array_len = 32768 * 2; // 2^16
    let batch_size = 2048; // 2^11
    let ctx = create_context_3(partitions_len, array_len, batch_size).unwrap();

    c.bench_function("array-agg", |b| {
        b.to_async(Runtime::new().unwrap()).iter(|| {
            query3(
                ctx.clone(),
                "SELECT a, b, array_agg(c), sum(d) FROM t group by a, b",
            )
        })
    });

    // c.bench_function("no-array-agg", |b| {
    //     b.to_async(Runtime::new().unwrap()).iter(|| {
    //         query3(
    //             ctx.clone(),
    //             "SELECT a, b, sum(d) FROM t group by a, b",
    //         )
    //     })
    // });

}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_blah() {
        let partitions_len = 8;
        let array_len = 32768 * 2; // 2^16
        let batch_size = 2048; // 2^11
        let ctx = create_context_2(partitions_len, array_len, batch_size).unwrap();
        for _ in 0..100000 {
            let _ = query_2(
                &ctx,
                "SELECT a, b, array_agg(distinct), sum(d) \
                 FROM t",
            );
        }

    }
}
